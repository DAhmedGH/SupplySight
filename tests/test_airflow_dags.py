"""Validate DAG structure without requiring a host Airflow installation."""

from __future__ import annotations

import runpy
import sys
from datetime import date
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DAG_FILE = PROJECT_ROOT / "airflow/dags/supplysight_dev.py"


class FakeDAG:
    active: FakeDAG | None = None

    def __init__(self, dag_id: str, **kwargs: object) -> None:
        self.dag_id = dag_id
        self.options = kwargs
        self.tasks: dict[str, FakeOperator] = {}

    def __enter__(self) -> FakeDAG:
        FakeDAG.active = self
        return self

    def __exit__(self, *_args: object) -> None:
        FakeDAG.active = None


class FakeOperator:
    def __init__(self, task_id: str, **kwargs: object) -> None:
        self.task_id = task_id
        self.options = kwargs
        self.upstream_task_ids: set[str] = set()
        self.downstream_task_ids: set[str] = set()
        assert FakeDAG.active is not None
        FakeDAG.active.tasks[task_id] = self

    def _link(self, other: FakeOperator) -> None:
        self.downstream_task_ids.add(other.task_id)
        other.upstream_task_ids.add(self.task_id)

    def __rshift__(self, other: FakeOperator | list[FakeOperator]):
        for task in other if isinstance(other, list) else [other]:
            self._link(task)
        return other

    def __rrshift__(self, other: list[FakeOperator]):
        for task in other:
            task._link(self)
        return self


class FakeAirflowSkipException(Exception):
    pass


@pytest.fixture
def dags(monkeypatch: pytest.MonkeyPatch) -> dict[str, object]:
    modules = {
        name: ModuleType(name)
        for name in (
            "airflow",
            "airflow.exceptions",
            "airflow.operators",
            "airflow.operators.bash",
            "airflow.operators.empty",
            "airflow.operators.python",
            "airflow.operators.trigger_dagrun",
            "airflow.utils",
            "airflow.utils.trigger_rule",
        )
    }
    modules["airflow"].DAG = FakeDAG
    modules["airflow.exceptions"].AirflowSkipException = FakeAirflowSkipException
    modules["airflow.operators.bash"].BashOperator = FakeOperator
    modules["airflow.operators.empty"].EmptyOperator = FakeOperator
    modules["airflow.operators.python"].BranchPythonOperator = FakeOperator
    modules["airflow.operators.python"].PythonOperator = FakeOperator
    modules["airflow.operators.trigger_dagrun"].TriggerDagRunOperator = (
        FakeOperator
    )
    modules["airflow.utils.trigger_rule"].TriggerRule = SimpleNamespace(
        NONE_FAILED_MIN_ONE_SUCCESS="none_failed_min_one_success"
    )
    for name, module in modules.items():
        monkeypatch.setitem(sys.modules, name, module)
    return runpy.run_path(str(DAG_FILE))


def test_ingestion_requires_preflight_and_dev_guard(dags: dict[str, object]) -> None:
    dag = dags["ingestion_dag"]
    assert dag.dag_id == "supplysight_dev_ingestion"
    assert dag.options["schedule"] is None
    assert dag.options["max_active_runs"] == 1
    tasks = dag.tasks
    assert tasks["choose_source"].downstream_task_ids == {
        "generate_dev_data",
        "use_existing_data",
    }
    assert tasks["source_ready"].upstream_task_ids == {
        "generate_dev_data",
        "use_existing_data",
    }
    assert tasks["ingest_raw"].upstream_task_ids == {
        "preflight_raw",
        "check_dev_target",
    }
    assert tasks["validate_raw"].upstream_task_ids == {"ingest_raw"}
    assert tasks["trigger_warehouse"].upstream_task_ids == {"validate_raw"}
    assert tasks["ingest_raw"].options["retries"] == 1
    assert tasks["ingest_raw"].options["pool"] == dags["MUTATION_POOL"]
    assert tasks["preflight_raw"].options.get("retries", 0) == 0


def test_warehouse_holds_shared_pool_for_full_build(dags: dict[str, object]) -> None:
    dag = dags["warehouse_dag"]
    assert dag.dag_id == "supplysight_dev_warehouse"
    assert dag.options["schedule"] is None
    assert dag.options["max_active_runs"] == 1
    assert set(dag.tasks) == {
        "warehouse_refresh",
        "forecast_demand",
        "inventory_intelligence",
    }
    task = dag.tasks["warehouse_refresh"]
    assert task.options["pool"] == dags["MUTATION_POOL"]
    assert task.options.get("retries", 0) == 0
    forecast = dag.tasks["forecast_demand"]
    assert forecast.options["pool"] == dags["MUTATION_POOL"]
    assert forecast.upstream_task_ids == {"warehouse_refresh"}
    inventory = dag.tasks["inventory_intelligence"]
    assert inventory.options["pool"] == dags["MUTATION_POOL"]
    assert inventory.options.get("retries", 0) == 0
    assert inventory.options["execution_timeout"].total_seconds() == 7200
    assert inventory.upstream_task_ids == {"forecast_demand"}
    assert forecast.downstream_task_ids == {"inventory_intelligence"}
    assert task.downstream_task_ids == {"forecast_demand"}


def test_inventory_intelligence_runs_only_its_tag_and_stops_on_failure(
    dags: dict[str, object], monkeypatch: pytest.MonkeyPatch
) -> None:
    run = dags["_run_inventory_intelligence"]
    commands = []
    monkeypatch.setitem(
        run.__globals__, "_historical_forecast_is_compatible", lambda: True
    )

    def fake_run(command, **kwargs):
        commands.append((command, kwargs))
        if command[1] == "run":
            return
        raise RuntimeError("inventory intelligence tests failed")

    monkeypatch.setattr(run.__globals__["subprocess"], "run", fake_run)
    with pytest.raises(RuntimeError, match="inventory intelligence tests failed"):
        run()

    assert [command[0][1] for command in commands] == ["run", "test"]
    for command, kwargs in commands:
        assert command[0] == "dbt"
        assert command[-2:] == ["--select", "tag:inventory_intelligence"]
        assert kwargs["check"] is True
        assert kwargs["cwd"] == run.__globals__["PROJECT_DIR"]
    assert commands[0][1]["timeout"] == 3600
    assert commands[1][1]["timeout"] == 2700


def test_compatible_historical_forecast_runs_phase10_selection(
    dags: dict[str, object], monkeypatch: pytest.MonkeyPatch
) -> None:
    run = dags["_run_inventory_intelligence"]
    commands = []
    monkeypatch.setitem(
        run.__globals__, "_historical_forecast_is_compatible", lambda: True
    )
    monkeypatch.setattr(
        run.__globals__["subprocess"],
        "run",
        lambda command, **kwargs: commands.append((command, kwargs)),
    )

    run()

    assert [command[0][1] for command in commands] == ["run", "test"]
    assert all(
        command[-2:] == ["--select", "tag:inventory_intelligence"]
        and kwargs["check"] is True
        for command, kwargs in commands
    )


def test_incompatible_historical_forecast_skips_before_dbt(
    dags: dict[str, object], monkeypatch: pytest.MonkeyPatch
) -> None:
    run = dags["_run_inventory_intelligence"]
    commands = []
    monkeypatch.setitem(
        run.__globals__, "_historical_forecast_is_compatible", lambda: False
    )
    monkeypatch.setattr(
        run.__globals__["subprocess"],
        "run",
        lambda command, **kwargs: commands.append((command, kwargs)),
    )

    with pytest.raises(FakeAirflowSkipException, match="2024-12-31"):
        run()
    assert commands == []


def test_historical_forecast_compatibility_check_is_read_only(
    dags: dict[str, object], monkeypatch: pytest.MonkeyPatch
) -> None:
    helper = dags["_historical_forecast_is_compatible"]
    calls = []

    class Cursor:
        description = []

        def execute(self, query):
            calls.append(query)

        def fetchone(self):
            return (True,)

        def close(self):
            pass

    class Connection:
        def cursor(self):
            return Cursor()

        def close(self):
            pass

    def fake_connect(settings):
        assert settings.database == "SUPPLY_CHAIN_DEV"
        assert settings.warehouse == "SUPPLY_CHAIN_DEV_WH"
        return Connection()

    from supplysight.forecasting import repository

    monkeypatch.setattr(repository, "_connect", fake_connect)
    monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "dev_account")
    monkeypatch.setenv("SNOWFLAKE_USER", "dev_user")
    monkeypatch.setenv("SNOWFLAKE_ROLE", "SUPPLYSIGHT_DEV_SERVICE")
    monkeypatch.setenv("SNOWFLAKE_PRIVATE_KEY_FILE", "dev_key.p8")
    monkeypatch.setenv("SNOWFLAKE_DATABASE", "SUPPLY_CHAIN_DEV")
    monkeypatch.setenv("SNOWFLAKE_WAREHOUSE", "SUPPLY_CHAIN_DEV_WH")
    monkeypatch.setenv("SNOWFLAKE_SCHEMA", "RAW")

    assert helper() is True
    assert len(calls) == 1
    query = calls[0].upper()
    assert "SELECT" in query
    assert "CURRENT_FORECASTS" in query
    assert "RUN_METADATA" in query
    assert "2024-12-31" in query
    assert "2025-01-01" in query
    assert "2025-03-01" in query
    assert "INSERT" not in query
    assert "UPDATE" not in query
    assert "DELETE" not in query


def test_forecast_uses_complete_validated_source_period(
    dags: dict[str, object], monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    run = dags["_run_forecast"]
    report = tmp_path / "data/generated/dev-2024/generation_report.json"
    report.parent.mkdir(parents=True)
    report.write_text(
        '{"period": {"start_date": "2024-01-01", '
        '"end_date": "2024-12-31"}, "validation": {"valid": true}}',
        encoding="utf-8",
    )
    monkeypatch.setitem(run.__globals__, "PROJECT_DIR", tmp_path)
    commands = []
    monkeypatch.setattr(
        run.__globals__["subprocess"],
        "run",
        lambda command, **kwargs: commands.append((command, kwargs)),
    )
    run(as_of=date(2025, 1, 1))
    assert commands[0][0][1:3] == ["-m", "supplysight.forecasting"]
    assert commands[0][0][-4:] == [
        "--coverage-start", "2024-01-01", "--observed-through", "2024-12-31"
    ]
    assert commands[0][1]["check"] is True
    report.write_text(
        '{"period": {"start_date": "2024-01-01", '
        '"end_date": "2024-12-30"}, "validation": {"valid": true}}',
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="complete calendar months"):
        run(as_of=date(2025, 1, 1))
    assert len(commands) == 1
    report.write_text(
        '{"period": {"start_date": "2024-01-01", '
        '"end_date": "2024-12-31"}, "validation": {"valid": true}}',
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="coverage is stale"):
        run(as_of=date(2026, 9, 25))
    assert len(commands) == 1
    with pytest.raises(ValueError, match="coverage is stale"):
        run(as_of=date(2025, 2, 1))
    assert len(commands) == 1


def test_warehouse_build_runs_in_order_and_stops_on_failure(
    dags: dict[str, object], monkeypatch: pytest.MonkeyPatch
) -> None:
    run = dags["_run_warehouse"]
    events = []
    monkeypatch.setitem(
        run.__globals__, "_validate_handoff", lambda **_: events.append("handoff")
    )
    monkeypatch.setitem(
        run.__globals__, "_record_dbt_result", lambda: events.append("record")
    )

    def fake_run(command, **_kwargs):
        events.append(tuple(command))
        if command[1:3] == ["test", "--project-dir"] and "tag:core" in command:
            raise RuntimeError("core tests failed")

    monkeypatch.setattr(run.__globals__["subprocess"], "run", fake_run)
    with pytest.raises(RuntimeError, match="core tests failed"):
        run(dag_run=SimpleNamespace(conf={}))
    assert events[0] == "handoff"
    assert events[-1][1] == "test"
    assert "tag:core" in events[-1]
    assert "record" not in events
    assert [event[1] for event in events[1:] if isinstance(event, tuple)] == [
        "parse", "compile", "run", "test", "snapshot", "run", "test"
    ]


def test_branch_and_handoff_path_are_strict(dags: dict[str, object]) -> None:
    choose = dags["_choose_source"]
    assert choose(dag_run=SimpleNamespace(conf={"generate_data": True})) == (
        "generate_dev_data"
    )
    assert choose(dag_run=SimpleNamespace(conf={"generate_data": "true"})) == (
        "use_existing_data"
    )
    validate = dags["_validated_result_path"]
    with pytest.raises(ValueError):
        validate({"result_path": "../../secrets.json"})
    with pytest.raises(ValueError):
        validate({"result_path": "data/runs/ingestion/a.json; touch /tmp/x"})
    valid = validate({"result_path": "data/runs/ingestion/20240924T120000.json"})
    assert valid.name == "20240924T120000.json"


def test_target_guard_rejects_production_role(
    dags: dict[str, object], monkeypatch: pytest.MonkeyPatch
) -> None:
    values = {
        "SNOWFLAKE_ACCOUNT": "dev_account",
        "SNOWFLAKE_USER": "dev_user",
        "SNOWFLAKE_ROLE": "SUPPLYSIGHT_PROD_ROLE",
        "SNOWFLAKE_PRIVATE_KEY_FILE": "test_key.p8",
        "SNOWFLAKE_DATABASE": "SUPPLY_CHAIN_DEV",
        "SNOWFLAKE_WAREHOUSE": "SUPPLY_CHAIN_DEV_WH",
        "SNOWFLAKE_SCHEMA": "RAW",
    }
    for name, value in values.items():
        monkeypatch.setenv(name, value)
    with pytest.raises(ValueError, match="development Snowflake role"):
        dags["_check_dev_target"]()
