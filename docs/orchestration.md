# Airflow orchestration

## Scope and DAGs

Phase 7 adds manual DEV orchestration for the implemented Python, Snowflake, and dbt pipeline. Airflow calls existing commands and does not calculate source or warehouse business rules. The Linux container runs Airflow 2.10.5 with LocalExecutor and Postgres metadata storage. No DAG is scheduled automatically, and each mutating DAG permits one active run.

| DAG | Purpose | Dependency path |
| --- | --- | --- |
| `supplysight_dev_ingestion` | Optional local data generation, whole-dataset RAW load, per-batch audit | Choose generated or existing files → source ready → parallel file preflight and DEV target guard → atomic RAW ingestion → read-only RAW validation → trigger warehouse DAG. |
| `supplysight_dev_warehouse` | Build and validate analytical layers | One locked refresh task revalidates the specific RAW batch, then runs dbt parse, compile, staging run/test, product snapshot, intermediate and CORE run/test, MARTS run/test, quality reconciliations, and result recording in order. Successful forecasting precedes a historical-run compatibility check; the inventory-intelligence dbt selection and tests run only when that check passes. |

The ingestion DAG accepts `{"generate_data": true}` in a manual trigger to regenerate the deterministic DEV dataset in `data/generated/dev-2024`. Otherwise it consumes the existing files there. The warehouse DAG requires a `result_path` from a successful ingestion run, including when it is manually rerun after a warehouse failure. It rejects missing or unsafe paths and repeats the read-only batch validation before any dbt task. Default Airflow success dependencies prevent downstream transformations after a failed preflight, load, audit, or layer test. The ingestion DAG waits for the triggered warehouse run, so its final state reflects that run.

The nine RAW entities are loaded by the existing graph-wide preflight and one loader invocation. They are not split across Airflow tasks because their foreign-key dependencies and transactional load would be weakened. Source preflight and a local DEV target check run in parallel; dbt layers run in dependency order. Snapshotting precedes CORE, CORE validation precedes MARTS, and forecasting must succeed before Phase 10 starts. The `supplysight_dev_raw_warehouse` Airflow pool has one slot, assigned to the RAW load, warehouse refresh, forecasting, and inventory-intelligence tasks. This prevents overlapping DEV mutations. Airflow init creates the pool before scheduling either DAG.

## Execution policy and failures

The ingestion wrapper in `scripts/orchestrate_raw.py` exposes `preflight`, `ingest`, and `validate`. It reuses the existing generator/loader, writes an atomic JSON result under ignored `data/runs/ingestion/`, and validates the batch header, all nine file audits, source counts, current RAW lineage fields, and business-key uniqueness. The result holds batch ID, row totals, and DEV target; it holds no credentials. `SUPPLY_CHAIN_DEV.RAW` and `SUPPLY_CHAIN_DEV_WH` are checked by both the wrapper and existing Snowflake session guards. The dbt profile and schema macro pin dbt to DEV.

Local generation, preflight, and the warehouse refresh have no automatic retry. RAW ingestion has one retry after two minutes; ingestion is source-idempotent but a retry creates another batch audit and quarantine attempt. Every task has an execution timeout, from two minutes for the target guard to seven hours for the full warehouse refresh. The refresh stops at the first failed command and logs each layer's start and completion. Operators can rerun the warehouse DAG with the prior validated result path without reingesting. Airflow records the refresh task state and timing; its command-level logs identify the failed layer.

Airflow records DAG/run/task metadata in Postgres. The ignored ingestion result JSON records batch ID and counts; `SUPPLY_CHAIN_DEV.RAW.INGESTION_BATCHES` and `INGESTION_BATCH_FILES` remain the load-audit source of truth. The final warehouse task reads dbt's `run_results.json` and logs its invocation ID and status counts. Airflow and dbt logs should be kept free of private key contents and credentials.

## Local Docker setup

Docker Desktop with Compose is required to run the stack. Copy `airflow/.env.example` to ignored `airflow/.env`; set the local Airflow database/admin secrets, Fernet and webserver keys, DEV Snowflake account/user/role, and a host path to the private key. Use a DEV-only role. The database password is placed in a SQLAlchemy URL, so generate it from URL-unreserved characters such as hexadecimal digits. Copy `dbt/profiles.yml.example` to ignored `dbt/profiles.yml` if needed. The dbt profile reads the container's `SNOWFLAKE_*` environment variables and fixes database/warehouse/schema routing. The image keeps Airflow and the project dbt/Snowflake CLIs in separate Python environments to avoid conflicting dependency versions. Compose mounts the key read-only only into the scheduler, where LocalExecutor tasks run. The project is mounted at `/opt/supplysight`, and generated files, run results, dbt artifacts, and Airflow logs remain local and ignored by Git.

From the repository root:

```powershell
docker compose --env-file airflow/.env -f airflow/docker-compose.yml up --build airflow-init
docker compose --env-file airflow/.env -f airflow/docker-compose.yml up --build -d airflow-webserver airflow-scheduler
docker compose --env-file airflow/.env -f airflow/docker-compose.yml exec airflow-scheduler airflow dags list
docker compose --env-file airflow/.env -f airflow/docker-compose.yml exec airflow-scheduler airflow dags list-import-errors
```

The web UI listens only on `127.0.0.1:8080` by default. Trigger the ingestion DAG there with or without `generate_data`. The validated result path is passed automatically to the warehouse DAG. For a manual warehouse rerun, pass `{"result_path": "data/runs/ingestion/YYYYMMDDTHHMMSS.json"}` from the prior ingestion run. Do not trigger an unapproved live refresh.

## Approval and verification

Project command policy requires approval before the live RAW merge/DDL, dbt snapshot, and dbt runs; a manual Airflow trigger that reaches those tasks is the approval boundary. Read-only parse, compile, tests, and DEV audit queries may be run without that approval. Do not point the stack at `SUPPLY_CHAIN_PROD` or an administrative role.

The forecast task requires source coverage ending in the month immediately before execution. The fixed 2024 DEV sample is stale in 2026, so the DAG will not publish its January–March 2025 forecast as a current run. A successful fresh forecast replaces `CURRENT_FORECASTS`; the fixed-cutoff Phase 10 models cannot use that new run. The inventory task checks the current forecast run and explicitly skips its dbt commands unless it is the compatible 2024-12-31 run with the January–March 2025 horizon. Historical Phase 10 validation therefore uses a separately approved manual `tag:inventory_intelligence` dbt build against the persisted historical forecast; the freshness guard is not bypassed.

Verify DAG imports and dependency tests locally, then confirm the Compose services are healthy and both DAGs are visible. For an approved DEV execution, confirm the batch/result path, nine file audits, dbt task states, product snapshot, STAGING/CORE/MARTS test outcomes, final quality reconciliation, and dbt invocation ID. Query only `SUPPLY_CHAIN_DEV` to confirm relation targets and counts. The repository's existing layer runbooks define source, dimensional, and KPI semantics; Airflow does not replace those checks.
