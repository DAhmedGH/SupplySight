from __future__ import annotations

from copy import deepcopy
from datetime import date, datetime
from pathlib import Path

import pytest

from supplysight.forecasting.repository import (
    _verify_session,
    load_monthly_demand,
    persist_run,
    validate_target,
)
from supplysight.settings import ConfigurationError, SnowflakeSettings


class FakeCursor:
    def __init__(self, rows=()):
        self.rows = list(rows)
        columns = (
            "DEMAND_DATE",
            "PRODUCT_ID",
            "PRODUCT_KEY",
            "WAREHOUSE_KEY",
            "ORDERED_QUANTITY",
        )
        self.description = [(name,) for name in columns]
        self.calls = []

    def execute(self, sql, params=None):
        self.calls.append((sql, params))

    def executemany(self, sql, rows):
        self.calls.append((sql, list(rows)))

    def fetchall(self):
        return self.rows

    def fetchone(self):
        return self.rows[0] if self.rows else None

    def close(self):
        pass


class FakeConnection:
    def __init__(self, rows=()):
        self.fake_cursor = FakeCursor(rows)
        self.committed = False
        self.rolled_back = False

    def cursor(self):
        return self.fake_cursor

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True

    def close(self):
        pass


def settings(database="SUPPLY_CHAIN_DEV", schema="RAW", role="SUPPLYSIGHT_DEV_SERVICE"):
    return SnowflakeSettings(
        account="test",
        user="test",
        role=role,
        private_key_file=Path("key.p8"),
        database=database,
        warehouse="SUPPLY_CHAIN_DEV_WH",
        schema=schema,
    )


def test_target_guard_rejects_non_dev_before_connection():
    with pytest.raises(ConfigurationError, match="SUPPLY_CHAIN_DEV"):
        validate_target(settings(database="SUPPLY_CHAIN_PROD"))
    with pytest.raises(ConfigurationError):
        validate_target(settings(schema="FORECASTING"))
    for role in (
        "ACCOUNTADMIN",
        "SUPPLYSIGHT_PROD",
        "OTHER_ROLE",
        "UNAPPROVED_DEV_ALIAS",
    ):
        with pytest.raises(ConfigurationError):
            validate_target(settings(role=role))


def test_forecast_role_error_names_canonical_role():
    with pytest.raises(ConfigurationError, match="SUPPLYSIGHT_DEV_SERVICE"):
        validate_target(settings(role="OTHER_ROLE"))


def test_active_session_context_is_checked():
    class SessionConnection(FakeConnection):
        def __init__(self, row):
            super().__init__()
            self.fake_cursor.rows = [row]

    _verify_session(
        SessionConnection(
            (
                "SUPPLY_CHAIN_DEV",
                "RAW",
                "SUPPLY_CHAIN_DEV_WH",
                "SUPPLYSIGHT_DEV_SERVICE"
            )
        ),
        settings(),
    )
    with pytest.raises(ConfigurationError, match="Active Snowflake session"):
        _verify_session(
            SessionConnection(
                (
                    "SUPPLY_CHAIN_PROD",
                    "RAW",
                    "SUPPLY_CHAIN_DEV_WH",
                    "SUPPLYSIGHT_DEV_SERVICE",
                )
            ),
            settings(),
        )


def test_read_uses_dev_marts_and_gross_order_quantity(monkeypatch):
    connection = FakeConnection(
        [(date(2025, 1, 1), "P1", "PK1", "WK1", 12.0)]
    )
    monkeypatch.setattr(
        "supplysight.forecasting.repository._connect", lambda _: connection
    )
    rows = load_monthly_demand(
        settings(), date(2025, 1, 1), date(2025, 2, 28)
    )
    assert rows == [
        {
            "demand_date": date(2025, 1, 1),
            "product_id": "P1",
            "product_key": "PK1",
            "warehouse_key": "WK1",
            "ordered_quantity": 12.0,
        }
    ]
    sql = connection.fake_cursor.calls[0][0]
    assert "SUPPLY_CHAIN_DEV.MARTS.SALES_ORDER_LINE" in sql
    assert "SUPPLY_CHAIN_DEV.CORE.DIM_PRODUCT" in sql
    assert "ORDER_STATUS" not in sql
    assert "QUALITY_STATUS IN ('CLEAN', 'WARNING')" in sql
    assert "SUPPLY_CHAIN_PROD" not in sql


def test_persistence_replaces_current_and_run_scoped_evaluations(monkeypatch):
    connection = FakeConnection()
    monkeypatch.setattr(
        "supplysight.forecasting.repository._connect", lambda _: connection
    )
    forecast = {
        "forecast_date": date(2025, 4, 1),
        "product_id": "P1",
        "product_key": "PK1",
        "warehouse_key": "WK1",
        "horizon": 1,
        "predicted_demand": 3.0,
        "lower_bound": 0.0,
        "upper_bound": 5.0,
        "model_name": "moving_average",
        "fallback": True,
        "training_start": date(2024, 1, 1),
        "training_end": date(2025, 3, 31),
        "generated_at": datetime(2025, 4, 1),
        "source_version": "abc123",
    }
    evaluation = {
        "product_id": "P1",
        "warehouse_key": "WK1",
        "model_name": "moving_average",
        "mae": 2.0,
        "rmse": 2.5,
        "wape": 0.2,
        "test_mae": 2.1,
        "test_rmse": 2.6,
        "test_wape": 0.21,
        "selected": True,
        "fallback": True,
        "validation_period_start": date(2025, 1, 1),
        "validation_period_end": date(2025, 3, 1),
    }
    metadata = {
        "started_at": datetime(2025, 4, 1),
        "completed_at": datetime(2025, 4, 1, 0, 1),
        "status": "SUCCEEDED",
        "training_start": date(2024, 1, 1),
        "training_end": date(2025, 3, 31),
        "horizon": 3,
        "series_attempted": 1,
        "series_successful": 1,
        "series_fallback": 1,
        "series_failed": 0,
        "records_produced": 3,
        "source_version": "abc123",
    }
    persist_run(
        settings(),
        run_id="run-1",
        forecasts=[forecast],
        evaluations=[evaluation],
        run_metadata=metadata,
    )
    calls = connection.fake_cursor.calls
    assert calls[1][0].endswith("CURRENT_FORECASTS")
    assert calls[2][0].endswith("MODEL_EVALUATION WHERE RUN_ID = %s")
    assert calls[3][0].endswith("RUN_METADATA WHERE RUN_ID = %s")
    assert connection.committed
    assert not connection.rolled_back
    assert calls[4][1][0][0] == "run-1"
    assert calls[5][1][0][0] == "run-1"


def test_persistence_rolls_back_failed_transaction(monkeypatch):
    connection = FakeConnection()

    def fail_insert(sql, rows):
        raise RuntimeError("write failed")

    connection.fake_cursor.executemany = fail_insert
    monkeypatch.setattr(
        "supplysight.forecasting.repository._connect", lambda _: connection
    )
    with pytest.raises(RuntimeError, match="write failed"):
        persist_run(
            settings(),
            run_id="run-2",
            forecasts=[{"product_id": "P1"}],
            evaluations=[],
            run_metadata={},
        )
    assert connection.rolled_back
    assert connection.committed
    assert any(
        "RUN_METADATA" in sql and params[3] == "FAILED"
        for sql, params in connection.fake_cursor.calls
        if "INSERT INTO" in sql
    )


def test_failed_same_id_retry_preserves_successful_publication(monkeypatch):
    tables = {
        name: []
        for name in ("CURRENT_FORECASTS", "MODEL_EVALUATION", "RUN_METADATA")
    }

    class TransactionalConnection:
        def __init__(self):
            self.work = None
            self.fail_evaluation = False
            self.selected = None

        def cursor(self):
            return self

        def execute(self, sql, params=None):
            nonlocal tables
            statement = " ".join(sql.upper().split())
            if statement == "BEGIN":
                self.work = deepcopy(tables)
            elif statement.startswith("SELECT 1 FROM"):
                self.selected = next(
                    (row for row in self.work["RUN_METADATA"] if row[0] == params[0]),
                    None,
                )
            elif statement.startswith("DELETE FROM"):
                name = next(name for name in tables if name in statement)
                self.work[name] = (
                    [row for row in self.work[name] if row[0] != params[0]]
                    if params
                    else []
                )
            elif statement.startswith("INSERT INTO"):
                name = next(name for name in tables if name in statement)
                self.work[name].append(params)

        def executemany(self, sql, rows):
            name = next(name for name in tables if name in sql)
            if name == "MODEL_EVALUATION" and self.fail_evaluation:
                raise RuntimeError("injected evaluation write failure")
            self.work[name].extend(rows)

        def fetchone(self):
            return self.selected

        def commit(self):
            nonlocal tables
            tables = deepcopy(self.work)
            self.work = None

        def rollback(self):
            self.work = None

        def close(self):
            pass

    connection = TransactionalConnection()
    monkeypatch.setattr(
        "supplysight.forecasting.repository._connect", lambda _: connection
    )
    metadata = {"horizon": 3, "series_attempted": 1, "series_successful": 1}
    persist_run(
        settings(),
        run_id="deterministic-run",
        forecasts=[{"product_id": "P1", "predicted_demand": 4.0}],
        evaluations=[{"product_id": "P1", "warehouse_key": "W1", "mae": 2.0}],
        run_metadata=metadata,
    )
    published = deepcopy(tables)
    assert published["RUN_METADATA"][0][3] == "SUCCEEDED"

    connection.fail_evaluation = True
    with pytest.raises(RuntimeError, match="injected evaluation write failure"):
        persist_run(
            settings(),
            run_id="deterministic-run",
            forecasts=[{"product_id": "P1", "predicted_demand": 99.0}],
            evaluations=[{"product_id": "P1", "warehouse_key": "W1", "mae": 99.0}],
            run_metadata=metadata,
        )
    assert tables == published

    persist_run(
        settings(),
        run_id="deterministic-run",
        forecasts=[],
        evaluations=[],
        run_metadata=metadata,
        status="FAILED",
        failure_reason="computation failed",
    )
    assert tables == published


def test_cli_uses_core_deterministic_run_id_when_not_provided(monkeypatch):
    from supplysight.forecasting import cli

    rows = [
        {
            "product_id": "P1",
            "product_key": "PK1",
            "warehouse_key": "WK1",
            "demand_date": date(2024, month, 1),
            "ordered_quantity": float(month % 4 + 1),
        }
        for month in range(1, 13)
    ]
    saved_ids = []
    monkeypatch.setattr(cli, "load_dotenv", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        cli.SnowflakeSettings,
        "from_environment",
        classmethod(lambda cls: settings()),
    )
    monkeypatch.setattr(cli, "load_monthly_demand", lambda *args: rows)
    monkeypatch.setattr(
        cli,
        "persist_run",
        lambda settings, **kwargs: saved_ids.append(kwargs["run_id"]),
    )

    arguments = [
        "--coverage-start",
        "2024-01-01",
        "--observed-through",
        "2024-12-31",
    ]
    assert cli.main(arguments) == 0
    assert cli.main(arguments) == 0
    assert saved_ids[0] == saved_ids[1]
    assert len(saved_ids[0]) == 36


@pytest.mark.parametrize("horizon", [0, 1, 2, 4, 6])
def test_cli_rejects_other_horizons_before_loading_configuration(
    monkeypatch, horizon
):
    from supplysight.forecasting import cli

    monkeypatch.setattr(
        cli,
        "load_dotenv",
        lambda *args, **kwargs: pytest.fail("configuration should not be loaded"),
    )
    with pytest.raises(SystemExit, match="three-month forecast horizon"):
        cli.main(
            [
                "--coverage-start", "2024-01-01",
                "--observed-through", "2024-12-31",
                "--horizon", str(horizon),
            ]
        )
