"""Manual development ingestion and warehouse orchestration."""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from calendar import monthrange
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from airflow.exceptions import AirflowSkipException
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule

from airflow import DAG

PROJECT_DIR = Path(os.environ.get("SUPPLYSIGHT_PROJECT_DIR", "/opt/supplysight"))
PROJECT_PYTHON = os.environ.get("SUPPLYSIGHT_PYTHON", sys.executable)
INPUT_DIR = Path("data/generated/dev-2024")
RESULT_TEMPLATE = "data/runs/ingestion/{{ ts_nodash }}.json"
START_DATE = datetime(2024, 1, 1, tzinfo=timezone.utc)
DBT_ARGS = ("--project-dir", "dbt", "--profiles-dir", "dbt")
MUTATION_POOL = "supplysight_dev_raw_warehouse"


def _choose_source(**context: object) -> str:
    """Generate only when a manual run explicitly requests it."""
    dag_run = context["dag_run"]
    return (
        "generate_dev_data"
        if dag_run.conf.get("generate_data") is True
        else "use_existing_data"
    )


def _check_dev_target() -> dict[str, str]:
    """Reject an unsafe connection target before the loader can mutate RAW."""
    from supplysight.settings import SnowflakeSettings

    settings = SnowflakeSettings.from_environment()
    if "PROD" in settings.role.upper() or "ACCOUNTADMIN" in settings.role.upper():
        raise ValueError("A development Snowflake role is required.")
    return {
        "database": settings.database,
        "schema": settings.schema,
        "warehouse": settings.warehouse,
    }


def _validated_result_path(conf: dict[str, object]) -> Path:
    value = conf.get("result_path")
    if not isinstance(value, str) or not re.fullmatch(
        r"data/runs/ingestion/\d{8}T\d{6}\.json", value
    ):
        raise ValueError("A validated ingestion result_path is required.")
    path = (PROJECT_DIR / value).resolve()
    allowed = (PROJECT_DIR / "data/runs/ingestion").resolve()
    if path.parent != allowed:
        raise ValueError("Ingestion result_path must stay in data/runs/ingestion.")
    return path


def _validate_handoff(**context: object) -> dict[str, object]:
    """Recheck the specific RAW batch before a warehouse refresh."""
    dag_run = context["dag_run"]
    result_path = _validated_result_path(dag_run.conf)
    subprocess.run(
        [
            PROJECT_PYTHON,
            "scripts/orchestrate_raw.py",
            "validate",
            "--input-dir",
            str(INPUT_DIR),
            "--result-path",
            str(result_path),
        ],
        cwd=PROJECT_DIR,
        check=True,
        timeout=600,
    )
    result = json.loads(result_path.read_text(encoding="utf-8"))
    return {
        "batch_id": result["batch_id"],
        "source_rows": result["source_rows"],
        "database": result["database"],
        "warehouse": result["warehouse"],
    }


def _record_dbt_result(**context: object) -> dict[str, object]:
    """Publish the final dbt test invocation summary to the task log and XCom."""
    path = PROJECT_DIR / "dbt/target/run_results.json"
    result = json.loads(path.read_text(encoding="utf-8"))
    statuses: dict[str, int] = {}
    for node in result.get("results", []):
        status = node.get("status", "unknown")
        statuses[status] = statuses.get(status, 0) + 1
    summary = {
        "invocation_id": result["metadata"]["invocation_id"],
        "generated_at": result["metadata"]["generated_at"],
        "status_counts": statuses,
    }
    print(json.dumps(summary, sort_keys=True))
    return summary


def _run_warehouse(**context: object) -> dict[str, object]:
    """Hold the shared mutation slot throughout validation and the dbt build."""
    handoff = _validate_handoff(**context)
    commands = (
        ("parse", ("parse",), 600),
        ("compile", ("compile",), 1800),
        ("staging run", ("run", "--select", "tag:staging"), 1800),
        (
            "staging test",
            ("test", "--select", "tag:staging", "--indirect-selection", "cautious"),
            1800,
        ),
        ("product snapshot", ("snapshot", "--select", "snap_products"), 1800),
        (
            "intermediate and core run",
            ("run", "--select", "tag:intermediate", "tag:core"),
            3600,
        ),
        (
            "intermediate and core test",
            (
                "test", "--select", "tag:intermediate", "tag:core",
                "--indirect-selection", "cautious",
            ),
            2700,
        ),
        ("marts run", ("run", "--select", "tag:marts"), 3600),
        ("marts test", ("test", "--select", "tag:marts"), 2700),
        (
            "quality reconciliations",
            (
                "test",
                "--select",
                "marts_core_reconciliation",
                "marts_fallback_reconciliation",
                "marts_inventory_demand_reconciliation",
                "marts_executive_reconciliation",
            ),
            1200,
        ),
    )
    for label, args, timeout in commands:
        command = ["dbt", args[0], *DBT_ARGS, *args[1:]]
        print(f"Starting {label}: {' '.join(command)}", flush=True)
        subprocess.run(command, cwd=PROJECT_DIR, check=True, timeout=timeout)
        print(f"Completed {label}", flush=True)
    return {"handoff": handoff, "dbt": _record_dbt_result()}


def _forecast_coverage(as_of: date | None = None) -> tuple[str, str]:
    """Use the validated source period as the complete demand window."""
    report_path = PROJECT_DIR / INPUT_DIR / "generation_report.json"
    report = json.loads(report_path.read_text(encoding="utf-8"))
    period = report["period"]
    start = datetime.strptime(period["start_date"], "%Y-%m-%d").date()
    end = datetime.strptime(period["end_date"], "%Y-%m-%d").date()
    if start.day != 1 or end.day != monthrange(end.year, end.month)[1]:
        raise ValueError("Forecast source coverage must span complete calendar months.")
    if start > end or report.get("validation", {}).get("valid") is not True:
        raise ValueError("A valid source coverage report is required for forecasting.")
    current_month = as_of or datetime.now(timezone.utc).date()
    month_index = current_month.year * 12 + current_month.month
    if end.year * 12 + end.month >= month_index:
        raise ValueError("Forecast source coverage must end before the current month.")
    if end.year * 12 + end.month + 1 < month_index:
        raise ValueError(
            "Forecast source coverage is stale: its first forecast month "
            "is before the current month."
        )
    return start.isoformat(), end.isoformat()


def _run_forecast(as_of: date | None = None) -> None:
    """Forecast after the warehouse refresh has completed successfully."""
    coverage_start, observed_through = _forecast_coverage(as_of=as_of)
    subprocess.run(
        [
            PROJECT_PYTHON,
            "-m",
            "supplysight.forecasting",
            "--coverage-start",
            coverage_start,
            "--observed-through",
            observed_through,
        ],
        cwd=PROJECT_DIR,
        check=True,
        timeout=7200,
    )


def _run_inventory_intelligence() -> None:
    """Build and test inventory models for the compatible historical forecast."""
    if not _historical_forecast_is_compatible():
        raise AirflowSkipException(
            "Inventory intelligence requires one successful current forecast run "
            "observed through 2024-12-31 with a three-month Jan-Mar 2025 horizon."
        )

    commands = (
        ("inventory intelligence run", "run", 3600),
        ("inventory intelligence test", "test", 2700),
    )
    for label, subcommand, timeout in commands:
        command = [
            "dbt",
            subcommand,
            *DBT_ARGS,
            "--select",
            "tag:inventory_intelligence",
        ]
        print(f"Starting {label}: {' '.join(command)}", flush=True)
        subprocess.run(command, cwd=PROJECT_DIR, check=True, timeout=timeout)
        print(f"Completed {label}", flush=True)


def _historical_forecast_is_compatible() -> bool:
    """Read-only check that CURRENT_FORECASTS has the historical planning run."""
    from supplysight.forecasting.repository import _connect
    from supplysight.settings import SnowflakeSettings

    query = """
        WITH current_runs AS (
            SELECT COUNT(DISTINCT RUN_ID) AS RUN_COUNT,
                   MIN(RUN_ID) AS RUN_ID,
                   COUNT(*) AS FORECAST_ROW_COUNT,
                   COUNT(DISTINCT DATE_TRUNC('MONTH', FORECAST_DATE)) AS MONTH_COUNT,
                   MIN(FORECAST_DATE) AS FIRST_FORECAST_DATE,
                   MAX(FORECAST_DATE) AS LAST_FORECAST_DATE,
                   COUNT_IF(
                       FORECAST_DATE < TO_DATE('2025-01-01')
                       OR FORECAST_DATE >= TO_DATE('2025-04-01')
                       OR FORECAST_DATE <> DATE_TRUNC('MONTH', FORECAST_DATE)
                   ) AS OUT_OF_SCOPE_ROW_COUNT
            FROM SUPPLY_CHAIN_DEV.FORECASTING.CURRENT_FORECASTS
        ), duplicate_groups AS (
            SELECT COUNT(*) AS DUPLICATE_GROUP_COUNT
            FROM (
                SELECT RUN_ID, FORECAST_DATE, PRODUCT_ID, WAREHOUSE_KEY
                FROM SUPPLY_CHAIN_DEV.FORECASTING.CURRENT_FORECASTS
                GROUP BY RUN_ID, FORECAST_DATE, PRODUCT_ID, WAREHOUSE_KEY
                HAVING COUNT(*) > 1
            )
        ), compatible_metadata AS (
            SELECT COUNT(*) AS MATCHING_METADATA_COUNT
            FROM SUPPLY_CHAIN_DEV.FORECASTING.RUN_METADATA m
            JOIN current_runs r ON m.RUN_ID = r.RUN_ID
            WHERE UPPER(m.STATUS) = 'SUCCEEDED'
              AND m.OBSERVED_THROUGH = TO_DATE('2024-12-31')
              AND m.HORIZON = 3
        )
        SELECT r.RUN_COUNT = 1
                   AND r.FORECAST_ROW_COUNT > 0
                   AND r.MONTH_COUNT = 3
                   AND r.FIRST_FORECAST_DATE = TO_DATE('2025-01-01')
                   AND r.LAST_FORECAST_DATE = TO_DATE('2025-03-01')
                   AND COALESCE(r.OUT_OF_SCOPE_ROW_COUNT, 0) = 0
                   AND d.DUPLICATE_GROUP_COUNT = 0
                   AND m.MATCHING_METADATA_COUNT = 1 AS IS_COMPATIBLE
        FROM current_runs r
        CROSS JOIN duplicate_groups d
        CROSS JOIN compatible_metadata m
    """
    connection = _connect(SnowflakeSettings.from_environment())
    try:
        cursor = connection.cursor()
        try:
            cursor.execute(query)
            result = cursor.fetchone()
            return bool(result and result[0])
        finally:
            cursor.close()
    finally:
        connection.close()


with DAG(
    dag_id="supplysight_dev_ingestion",
    description=(
        "Optional local generation, atomic RAW load, and per-batch DEV validation."
    ),
    schedule=None,
    start_date=START_DATE,
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "supplysight", "retries": 0},
    tags=["supplysight", "dev", "ingestion"],
) as ingestion_dag:
    choose_source = BranchPythonOperator(
        task_id="choose_source", python_callable=_choose_source
    )
    generate_dev_data = BashOperator(
        task_id="generate_dev_data",
        bash_command=(
            "python -m supplysight.data_generation --profile dev "
            f"--output-dir {INPUT_DIR}"
        ),
        cwd=str(PROJECT_DIR),
        execution_timeout=timedelta(minutes=20),
    )
    use_existing_data = EmptyOperator(task_id="use_existing_data")
    source_ready = EmptyOperator(
        task_id="source_ready",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )
    preflight_raw = BashOperator(
        task_id="preflight_raw",
        bash_command=(
            f"python scripts/orchestrate_raw.py preflight --input-dir {INPUT_DIR}"
        ),
        cwd=str(PROJECT_DIR),
        execution_timeout=timedelta(minutes=15),
    )
    check_dev_target = PythonOperator(
        task_id="check_dev_target",
        python_callable=_check_dev_target,
        execution_timeout=timedelta(minutes=2),
    )
    ingest_raw = BashOperator(
        task_id="ingest_raw",
        bash_command=(
            f"python scripts/orchestrate_raw.py ingest --input-dir {INPUT_DIR} "
            f"--result-path {RESULT_TEMPLATE}"
        ),
        cwd=str(PROJECT_DIR),
        retries=1,
        retry_delay=timedelta(minutes=2),
        retry_exponential_backoff=True,
        execution_timeout=timedelta(minutes=60),
        pool=MUTATION_POOL,
    )
    validate_raw = BashOperator(
        task_id="validate_raw",
        bash_command=(
            "python scripts/orchestrate_raw.py validate "
            f"--input-dir {INPUT_DIR} --result-path {RESULT_TEMPLATE}"
        ),
        cwd=str(PROJECT_DIR),
        execution_timeout=timedelta(minutes=15),
    )
    trigger_warehouse = TriggerDagRunOperator(
        task_id="trigger_warehouse",
        trigger_dag_id="supplysight_dev_warehouse",
        conf={"result_path": RESULT_TEMPLATE},
        wait_for_completion=True,
        poke_interval=20,
        execution_timeout=timedelta(hours=3),
    )

    choose_source >> [generate_dev_data, use_existing_data]
    [generate_dev_data, use_existing_data] >> source_ready
    source_ready >> [preflight_raw, check_dev_target]
    [preflight_raw, check_dev_target] >> ingest_raw
    ingest_raw >> validate_raw >> trigger_warehouse


with DAG(
    dag_id="supplysight_dev_warehouse",
    description="Validated RAW through MARTS and monthly demand forecasting.",
    schedule=None,
    start_date=START_DATE,
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "supplysight", "retries": 0},
    tags=["supplysight", "dev", "warehouse"],
) as warehouse_dag:
    warehouse_refresh = PythonOperator(
        task_id="warehouse_refresh",
        python_callable=_run_warehouse,
        pool=MUTATION_POOL,
        execution_timeout=timedelta(hours=7),
    )
    forecast_demand = PythonOperator(
        task_id="forecast_demand",
        python_callable=_run_forecast,
        pool=MUTATION_POOL,
        execution_timeout=timedelta(hours=2),
    )
    inventory_intelligence = PythonOperator(
        task_id="inventory_intelligence",
        python_callable=_run_inventory_intelligence,
        pool=MUTATION_POOL,
        execution_timeout=timedelta(hours=2),
    )
    warehouse_refresh >> forecast_demand >> inventory_intelligence
