# Setup and reproduction

The validated local path is Windows with Python 3.13.7. `requirements.lock` was validated on that version and operating system; other platforms need their own dependency validation. Run commands from the repository root. The local checks below do not connect to Snowflake or rebuild DEV objects.

## Quick local validation

Install Git, Python 3.13.7, and Docker Desktop with the WSL 2 backend if running the container checks. Clone the repository and create the environment:

```powershell
git clone <repository-url>
cd <repository-directory>
py -3.13 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.lock
.\.venv\Scripts\python.exe -m pip install --no-build-isolation -e .
.\.venv\Scripts\python.exe -m pip check
.\.venv\Scripts\python.exe -m ruff check src scripts tests
.\.venv\Scripts\python.exe -m pytest -p no:cacheprovider
```

The editable install makes `supplysight` importable; the lock pins its third-party dependencies. For credential-free dbt parsing, copy `dbt/profiles.yml.example` to an ignored local `dbt/profiles.yml`, then set placeholder variables in the current PowerShell session:

```powershell
Copy-Item dbt/profiles.yml.example dbt/profiles.yml
$env:SNOWFLAKE_ACCOUNT = 'placeholder_account'
$env:SNOWFLAKE_USER = 'placeholder_user'
$env:SNOWFLAKE_ROLE = 'placeholder_dev_role'
$env:SNOWFLAKE_PRIVATE_KEY_FILE = 'C:/placeholder/no-key.p8'
.\.venv\Scripts\dbt.exe parse --project-dir dbt --profiles-dir dbt --no-partial-parse
```

No real private key is needed for parse. Replace these placeholders before any live DEV operation. `dbt compile` for this project connects to Snowflake, even with `--no-introspect`.

With Docker running, validate Compose interpolation using placeholder values. Compose config does not start services; the host key path only needs to be a syntactically valid value for this check:

```powershell
$env:AIRFLOW_FERNET_KEY = 'validation-only'
$env:AIRFLOW_WEBSERVER_SECRET_KEY = 'validation-only'
$env:AIRFLOW_DB_USER = 'validation_user'
$env:AIRFLOW_DB_PASSWORD = 'validation_password'
$env:AIRFLOW_ADMIN_PASSWORD = 'validation_admin_password'
$env:SNOWFLAKE_PRIVATE_KEY_HOST_PATH = 'C:/placeholder/no-key.p8'
docker compose --file airflow/docker-compose.yml config --quiet
docker build --file airflow/Dockerfile --tag supplysight-airflow:validation .
docker run --rm --entrypoint /usr/local/bin/python --volume "${PWD}:/opt/supplysight:ro" --env PYTHONPATH=/opt/supplysight/src --env AIRFLOW__CORE__DAGS_FOLDER=/opt/supplysight/airflow/dags supplysight-airflow:validation -c "from airflow.models.dagbag import DagBag; bag = DagBag('/opt/supplysight/airflow/dags', include_examples=False); print(sorted(bag.dag_ids)); assert not bag.import_errors, bag.import_errors; assert {'supplysight_dev_ingestion', 'supplysight_dev_warehouse'} <= set(bag.dag_ids)"
```

These checks do not prove a live Snowflake build or Power BI refresh. The [CI runbook](ci_cd.md) describes the matching automated jobs.

## Full DEV environment and historical build

This path requires an existing Snowflake account, `SUPPLY_CHAIN_DEV`, `SUPPLY_CHAIN_DEV_WH`, the project schemas (`RAW`, `STAGING`, `INTERMEDIATE`, `CORE`, `MARTS`, `FORECASTING`, and `MONITORING` where configured), the `SUPPLYSIGHT_DEV_SERVICE` role with the required object permissions, and RSA private-key authentication. The repository does not provision the account or grant roles. Use no production target. Power BI Desktop is required to open and validate the report. Docker Desktop and WSL 2 are required only to run local Airflow on Windows.

Power BI uses the separate `POWERBI_READER` role. It needs `USAGE` on `SUPPLY_CHAIN_DEV_WH`, `SUPPLY_CHAIN_DEV`, and the `CORE`, `MARTS`, and `FORECASTING` schemas, plus `SELECT ON ALL TABLES` and `SELECT ON ALL VIEWS` for existing relations and `SELECT ON FUTURE TABLES` and `SELECT ON FUTURE VIEWS` in each of those schemas. Future SELECT grants keep dbt-created or recreated relations readable after a rebuild; existing-object grants cover relations already present. This setup grants the read role no RAW or STAGING access. See the [Power BI access matrix](power_bi.md#purpose-and-connection). The corrected DEV Desktop refresh succeeded with these grants.

Copy `.env.example` to ignored `.env` and set the DEV account, user, role, key path, and optional passphrase. Replace the placeholder dbt profile with the committed example; dbt reads `SNOWFLAKE_*` from the shell environment rather than loading `.env` itself. If running Airflow, copy `airflow/.env.example` to ignored `airflow/.env`, supply local Airflow secrets and the DEV key's host path, and use [the orchestration runbook](orchestration.md) for service startup. Keep keys and local profiles out of Git.

The fixed historical source is **synthetic 2024 operational history**, observed through **Dec 31, 2024**. Use one explicit path for generation, preflight, and ingestion:

```powershell
.\.venv\Scripts\python.exe -m supplysight.data_generation --profile dev --output-dir data/generated/dev-2024
.\.venv\Scripts\python.exe -c "from supplysight.ingestion import preflight_dataset; r = preflight_dataset('data/generated/dev-2024'); print(r.row_counts); print(r.warning_counts)"
.\.venv\Scripts\python.exe scripts/ingest_raw.py --input-dir data/generated/dev-2024 --env-file .env --quarantine-dir data/quarantine
```

Generation overwrites local ignored CSVs and the report. Preflight is local and read-only. **Ingestion creates missing `RAW` tables from `sql/raw/001_raw_tables.sql` and writes/merges DEV source, audit, and quarantine rows; it requires approval and DEV credentials.** It does not delete omitted source keys. Verify the batch and file audit records after the load.

With real `SNOWFLAKE_*` values exported to the shell and `dbt/profiles.yml` in place, run the warehouse in dependency order. `dbt parse` and `compile` inspect structure, while `snapshot` and `run` **write DEV relations and require approval**. `dbt test` queries existing DEV relations.

```powershell
.\.venv\Scripts\dbt.exe parse --project-dir dbt --profiles-dir dbt
.\.venv\Scripts\dbt.exe compile --project-dir dbt --profiles-dir dbt
.\.venv\Scripts\dbt.exe run --project-dir dbt --profiles-dir dbt --select tag:staging
.\.venv\Scripts\dbt.exe test --project-dir dbt --profiles-dir dbt --select tag:staging --indirect-selection cautious
.\.venv\Scripts\dbt.exe snapshot --project-dir dbt --profiles-dir dbt --select snap_products
.\.venv\Scripts\dbt.exe run --project-dir dbt --profiles-dir dbt --select tag:intermediate tag:core
.\.venv\Scripts\dbt.exe test --project-dir dbt --profiles-dir dbt --select tag:intermediate tag:core --indirect-selection cautious
.\.venv\Scripts\dbt.exe run --project-dir dbt --profiles-dir dbt --select tag:marts
.\.venv\Scripts\dbt.exe test --project-dir dbt --profiles-dir dbt --select tag:marts
```

Follow [RAW ingestion](raw_ingestion.md), [staging](staging.md), [dimensional warehouse](dimensional_warehouse.md), and [business marts](business_marts.md) for reconciliation checks. The forecast schema script `sql/forecasting/001_forecasting_tables.sql` creates DEV FORECASTING objects and must be executed only with approval. The forecast CLI reads MARTS in `--dry-run` mode; omitting that flag **writes DEV forecast tables**. The historical persisted run then supplies the fixed planning models:

```powershell
.\.venv\Scripts\python.exe -m supplysight.forecasting --coverage-start 2024-01-01 --observed-through 2024-12-31 --horizon 3 --dry-run
.\.venv\Scripts\python.exe -m supplysight.forecasting --coverage-start 2024-01-01 --observed-through 2024-12-31 --horizon 3
.\.venv\Scripts\dbt.exe run --project-dir dbt --profiles-dir dbt --select tag:inventory_intelligence
.\.venv\Scripts\dbt.exe test --project-dir dbt --profiles-dir dbt --select tag:inventory_intelligence
```

The persisted forecast command and inventory `dbt run` **write DEV data and require approval**. Verify the forecast run ID, 120 series × three forecast months, and planning run compatibility using [forecasting](forecasting.md) and [inventory intelligence](inventory_intelligence.md). Current code intentionally rejects stale 2024 data in the Airflow forecast task; the manual historical path above is not a live 2026 forecast. Do not advertise a manual Airflow trigger as a one-click rebuild of this fixed scenario.

Finally open `powerbi/SupplySight.pbip` in Power BI Desktop, set its `SnowflakeServer` parameter for the authorized DEV account, use the curated read-only `POWERBI_READER` access, and refresh. Power Query rejects incompatible forecast and planning lineage. The corrected historical DEV model has refreshed successfully in Desktop; compare a reproduced import with the [persisted DEV reconciliation](power_bi.md#reconciliation-and-limitations). Power BI credentials stay in the local credential store, not the repository.
