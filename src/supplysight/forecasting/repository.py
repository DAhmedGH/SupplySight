"""DEV-only Snowflake reads and persistence for demand forecast runs."""

from __future__ import annotations

import calendar
from dataclasses import asdict, is_dataclass
from datetime import date, datetime
from typing import Any, Iterable

from supplysight.settings import (
    DEVELOPMENT_DATABASE,
    DEVELOPMENT_WAREHOUSE,
    ConfigurationError,
    SnowflakeSettings,
)

FORECASTING_SCHEMA = "FORECASTING"
MARTS_SCHEMA = "MARTS"
CORE_SCHEMA = "CORE"
ALLOWED_DEV_ROLES = {"SUPPLYSIGHT_DEV_SERVICE"}
FORECAST_MODEL_VERSION = "monthly_demand_v1"


def validate_target(settings: SnowflakeSettings) -> None:
    """Fail closed unless the configured connector is scoped to DEV."""
    if (
        settings.database.upper() != DEVELOPMENT_DATABASE
        or settings.warehouse.upper() != DEVELOPMENT_WAREHOUSE
        or settings.schema.upper() != "RAW"
        or settings.role.upper() not in ALLOWED_DEV_ROLES
        or "PROD" in settings.role.upper()
        or "ACCOUNTADMIN" in settings.role.upper()
    ):
        raise ConfigurationError(
            "Forecasting requires SUPPLY_CHAIN_DEV / SUPPLY_CHAIN_DEV_WH; "
            "the configured schema must remain RAW and the role must be "
            "SUPPLYSIGHT_DEV_SERVICE."
        )


def _verify_session(connection: Any, settings: SnowflakeSettings) -> None:
    cursor = connection.cursor()
    try:
        cursor.execute(
            "SELECT CURRENT_DATABASE(), CURRENT_SCHEMA(), "
            "CURRENT_WAREHOUSE(), CURRENT_ROLE()"
        )
        row = cursor.fetchone()
    finally:
        cursor.close()
    expected = (
        DEVELOPMENT_DATABASE,
        "RAW",
        DEVELOPMENT_WAREHOUSE,
        settings.role.upper(),
    )
    actual = tuple(str(value or "").upper() for value in row or ())
    if actual != expected:
        raise ConfigurationError(
            "Active Snowflake session must be SUPPLY_CHAIN_DEV / RAW / "
            "SUPPLY_CHAIN_DEV_WH with the configured DEV role."
        )


def _connect(settings: SnowflakeSettings):
    validate_target(settings)
    import snowflake.connector

    connection = snowflake.connector.connect(**settings.connector_parameters())
    try:
        _verify_session(connection, settings)
    except Exception:
        connection.close()
        raise
    return connection


def load_monthly_demand(
    settings: SnowflakeSettings,
    coverage_start: date,
    observed_through: date,
) -> list[dict[str, Any]]:
    """Read gross eligible order-line demand from the mart by month and series."""
    if coverage_start > observed_through:
        raise ValueError("coverage_start must not be after observed_through")
    if coverage_start.day != 1:
        raise ValueError("coverage_start must be the first day of a month")
    month_end = calendar.monthrange(observed_through.year, observed_through.month)[1]
    if observed_through.day != month_end:
        raise ValueError("observed_through must be the last day of a month")
    sql = f"""
        SELECT DATE_TRUNC('MONTH', d.DATE_DAY)::DATE AS DEMAND_DATE,
               p.PRODUCT_ID,
               current_product.PRODUCT_KEY,
               s.WAREHOUSE_KEY,
               SUM(s.QUANTITY)::FLOAT AS ORDERED_QUANTITY
        FROM {DEVELOPMENT_DATABASE}.{MARTS_SCHEMA}.SALES_ORDER_LINE s
        JOIN {DEVELOPMENT_DATABASE}.{CORE_SCHEMA}.DIM_PRODUCT p
          ON s.PRODUCT_KEY = p.PRODUCT_KEY
        JOIN {DEVELOPMENT_DATABASE}.{CORE_SCHEMA}.DIM_PRODUCT current_product
          ON p.PRODUCT_ID = current_product.PRODUCT_ID
         AND current_product.IS_CURRENT = TRUE
        JOIN {DEVELOPMENT_DATABASE}.{CORE_SCHEMA}.DIM_DATE d
          ON s.ORDER_DATE_KEY = d.DATE_KEY
        WHERE d.DATE_DAY >= %s
          AND d.DATE_DAY < DATEADD(DAY, 1, %s)
          AND s.QUALITY_STATUS IN ('CLEAN', 'WARNING')
        GROUP BY 1, 2, 3, 4
        ORDER BY 1, 2, 4
    """
    connection = _connect(settings)
    try:
        cursor = connection.cursor()
        try:
            cursor.execute(sql, (coverage_start, observed_through))
            columns = [column[0].lower() for column in cursor.description]
            return [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]
        finally:
            cursor.close()
    finally:
        connection.close()


def _mapping(value: Any) -> dict[str, Any]:
    if is_dataclass(value):
        value = asdict(value)
    if isinstance(value, dict):
        return {str(key).lower(): item for key, item in value.items()}
    if hasattr(value, "_asdict"):
        return _mapping(value._asdict())
    raise TypeError(f"Expected forecast record mapping, received {type(value)!r}")


def _get(row: dict[str, Any], *names: str, default: Any = None) -> Any:
    for name in names:
        if name.lower() in row:
            return row[name.lower()]
    return default


def _record_values(row: Any) -> dict[str, Any]:
    return _mapping(row)


def _forecast_parameters(
    run_id: str, row: Any, defaults: dict[str, Any]
) -> tuple[Any, ...]:
    value = _record_values(row)
    return (
        run_id,
        _get(value, "generated_at", default=defaults["generated_at"]),
        _get(value, "forecast_date", "forecast_month", "period"),
        _get(value, "product_id"),
        _get(value, "product_key"),
        _get(value, "warehouse_key"),
        _get(value, "horizon", "forecast_horizon"),
        _get(value, "predicted_demand", "forecast", "yhat"),
        _get(value, "lower_bound", "prediction_lower"),
        _get(value, "upper_bound", "prediction_upper"),
        _get(value, "model_name", "model", default="unknown"),
        bool(_get(value, "fallback", "fallback_used", "is_fallback", default=False)),
        _get(value, "fallback_reason"),
        _get(value, "interval_method", "uncertainty_method"),
        _get(value, "sample_count", "residual_sample_count"),
        _get(value, "training_start", default=defaults["training_start"]),
        _get(value, "training_end", default=defaults["training_end"]),
        _get(value, "source_version", default=defaults["source_version"]),
        _get(value, "model_version", default=defaults["model_version"]),
    )


def _evaluation_parameters(
    run_id: str, row: Any, defaults: dict[str, Any]
) -> tuple[Any, ...]:
    value = _record_values(row)
    return (
        run_id,
        _get(value, "product_id"),
        _get(value, "warehouse_key"),
        _get(value, "model_name", "model", default="unknown"),
        _get(value, "mae"),
        _get(value, "rmse"),
        _get(value, "wape"),
        _get(value, "test_mae"),
        _get(value, "test_rmse"),
        _get(value, "test_wape"),
        _get(value, "baseline_mae"),
        _get(value, "baseline_rmse"),
        _get(value, "baseline_wape"),
        _get(value, "candidate_mae"),
        _get(value, "candidate_rmse"),
        _get(value, "candidate_wape"),
        _get(
            value,
            "selected_model",
            "selected_model_name",
            default=_get(value, "model_name", "model"),
        ),
        not bool(_get(value, "selected", default=False)),
        _get(value, "training_start", default=defaults["training_start"]),
        _get(value, "training_end", default=defaults["training_end"]),
        _get(value, "validation_period_start", "validation_start"),
        _get(value, "validation_period_end", "validation_end"),
    )


def _run_parameters(run: Any, run_id: str, status: str, error: str | None):
    value = _mapping(run) if run is not None else {}
    started = _get(value, "started_at", "generated_at", default=datetime.now())
    return (
        run_id,
        started,
        _get(value, "completed_at", "ended_at", default=datetime.now()),
        status.upper(),
        _get(value, "training_start", "coverage_start"),
        _get(value, "training_end", "observed_through"),
        _get(value, "horizon", "forecast_horizon"),
        int(_get(value, "series_attempted", "series_count", default=0)),
        int(
            _get(
                value,
                "series_successful",
                "series_succeeded",
                "successful_series",
                default=0,
            )
        ),
        int(_get(value, "series_fallback", "fallback_series", default=0)),
        int(_get(value, "series_failed", "failed_series", default=0)),
        int(_get(value, "records_produced", "forecast_count", default=0)),
        _get(value, "source_version", "code_version"),
        error,
        _get(value, "input_fingerprint"),
        _get(value, "coverage_start"),
        _get(value, "observed_through"),
        _get(value, "model_version", default=FORECAST_MODEL_VERSION),
    )


def _record_failed_run(
    cursor: Any,
    connection: Any,
    insert_metadata: str,
    run: dict[str, Any],
    run_id: str,
    failure_reason: str | None,
) -> None:
    """Record a new failure without changing an existing publication."""
    cursor.execute("BEGIN")
    cursor.execute(
        "SELECT 1 FROM "
        f"{DEVELOPMENT_DATABASE}.{FORECASTING_SCHEMA}.RUN_METADATA "
        "WHERE RUN_ID = %s LIMIT 1",
        (run_id,),
    )
    if cursor.fetchone() is None:
        cursor.execute(
            insert_metadata,
            _run_parameters(run, run_id, "FAILED", failure_reason),
        )
    connection.commit()


def persist_run(
    settings: SnowflakeSettings,
    *,
    run_id: str,
    forecasts: Iterable[Any],
    evaluations: Iterable[Any],
    run_metadata: Any,
    source_version: str | None = None,
    coverage_start: date | None = None,
    observed_through: date | None = None,
    status: str = "SUCCEEDED",
    failure_reason: str | None = None,
) -> None:
    """Atomically replace outputs for a run and publish its metadata."""
    validate_target(settings)
    status = {"SUCCESS": "SUCCEEDED"}.get(status.upper(), status.upper())
    if status not in {"SUCCEEDED", "PARTIAL", "FAILED"}:
        raise ValueError("status must be SUCCESS, SUCCEEDED, PARTIAL, or FAILED")
    forecasts = list(forecasts)
    evaluations = list(evaluations)
    if status == "FAILED" and (forecasts or evaluations):
        raise ValueError("FAILED runs cannot publish forecasts or evaluations")
    run = _mapping(run_metadata) if run_metadata is not None else {}
    defaults = {
        "generated_at": _get(run, "generated_at", "started_at", default=datetime.now()),
        "training_start": _get(run, "training_start", "coverage_start"),
        "training_end": _get(run, "training_end", "observed_through"),
        "source_version": source_version or _get(run, "source_version", "code_version"),
        "model_version": _get(run, "model_version", default=FORECAST_MODEL_VERSION),
        "coverage_start": coverage_start,
        "observed_through": observed_through,
    }

    insert_forecasts = f"""
        INSERT INTO {DEVELOPMENT_DATABASE}.{FORECASTING_SCHEMA}.CURRENT_FORECASTS
        (RUN_ID, GENERATED_AT, FORECAST_DATE, PRODUCT_ID, PRODUCT_KEY,
         WAREHOUSE_KEY, HORIZON, PREDICTED_DEMAND, LOWER_BOUND, UPPER_BOUND,
         MODEL_NAME, FALLBACK_USED, FALLBACK_REASON, UNCERTAINTY_METHOD,
         RESIDUAL_SAMPLE_COUNT, TRAINING_START, TRAINING_END, SOURCE_VERSION,
         MODEL_VERSION)
        VALUES ({', '.join(['%s'] * 19)})
    """
    insert_evaluations = f"""
        INSERT INTO {DEVELOPMENT_DATABASE}.{FORECASTING_SCHEMA}.MODEL_EVALUATION
        (RUN_ID, PRODUCT_ID, WAREHOUSE_KEY, MODEL_NAME, MAE, RMSE, WAPE,
         TEST_MAE, TEST_RMSE, TEST_WAPE, BASELINE_MAE, BASELINE_RMSE,
         BASELINE_WAPE, CANDIDATE_MAE, CANDIDATE_RMSE, CANDIDATE_WAPE, SELECTED_MODEL,
         FALLBACK_USED, TRAINING_START, TRAINING_END, VALIDATION_START,
         VALIDATION_END)
        VALUES ({', '.join(['%s'] * 22)})
    """
    insert_metadata = f"""
        INSERT INTO {DEVELOPMENT_DATABASE}.{FORECASTING_SCHEMA}.RUN_METADATA
        (RUN_ID, STARTED_AT, ENDED_AT, STATUS, TRAINING_START, TRAINING_END,
         HORIZON, SERIES_ATTEMPTED, SERIES_SUCCEEDED, SERIES_FALLBACK,
         SERIES_FAILED, RECORDS_PRODUCED, SOURCE_VERSION, FAILURE_REASON,
         INPUT_FINGERPRINT, COVERAGE_START, OBSERVED_THROUGH, MODEL_VERSION)
        VALUES ({', '.join(['%s'] * 18)})
    """
    metadata_values = {
        **run,
        "coverage_start": coverage_start,
        "observed_through": observed_through,
        "source_version": defaults["source_version"],
    }

    connection = _connect(settings)
    try:
        cursor = connection.cursor()
        try:
            if status == "FAILED":
                try:
                    _record_failed_run(
                        cursor,
                        connection,
                        insert_metadata,
                        metadata_values,
                        run_id,
                        failure_reason,
                    )
                except Exception:
                    connection.rollback()
                    raise
                return
            cursor.execute("BEGIN")
            cursor.execute(
                "DELETE FROM "
                f"{DEVELOPMENT_DATABASE}.{FORECASTING_SCHEMA}.CURRENT_FORECASTS"
            )
            cursor.execute(
                "DELETE FROM "
                f"{DEVELOPMENT_DATABASE}.{FORECASTING_SCHEMA}.MODEL_EVALUATION "
                "WHERE RUN_ID = %s",
                (run_id,),
            )
            cursor.execute(
                "DELETE FROM "
                f"{DEVELOPMENT_DATABASE}.{FORECASTING_SCHEMA}.RUN_METADATA "
                "WHERE RUN_ID = %s",
                (run_id,),
            )
            if forecasts:
                cursor.executemany(
                    insert_forecasts,
                    [_forecast_parameters(run_id, row, defaults) for row in forecasts],
                )
            if evaluations:
                cursor.executemany(
                    insert_evaluations,
                    [
                        _evaluation_parameters(run_id, row, defaults)
                        for row in evaluations
                    ],
                )
            cursor.execute(
                insert_metadata,
                _run_parameters(metadata_values, run_id, status, failure_reason),
            )
            connection.commit()
        except Exception as write_error:
            connection.rollback()
            failure = {
                **run,
                "completed_at": datetime.now(),
                "coverage_start": coverage_start,
                "observed_through": observed_through,
                "source_version": defaults["source_version"],
            }
            try:
                _record_failed_run(
                    cursor,
                    connection,
                    insert_metadata,
                    failure,
                    run_id,
                    (failure_reason or str(write_error))[:2000],
                )
            except Exception:
                connection.rollback()
            raise
        finally:
            cursor.close()
    finally:
        connection.close()
