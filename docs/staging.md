# dbt staging

## Scope and targets

Staging reads the nine current-state entity tables in `SUPPLY_CHAIN_DEV.RAW` and builds one view per entity in `SUPPLY_CHAIN_DEV.STAGING`. dbt uses `SUPPLY_CHAIN_DEV_WH`. The RAW tables remain owned by the Python ingestion workflow. Staging performs source-level normalization and casting; downstream business models consume the typed views.

Copy `dbt/profiles.yml.example` to the ignored local `dbt/profiles.yml`. The profile reads Snowflake account, user, role, key path, and optional key passphrase from the `SNOWFLAKE_*` shell environment. It fixes the database to `SUPPLY_CHAIN_DEV`, warehouse to `SUPPLY_CHAIN_DEV_WH`, and target schema to `STAGING`. The Python loader continues to use `RAW`. dbt sources explicitly name `SUPPLY_CHAIN_DEV.RAW`, independent of the model target schema.

## Model contract

Each `stg_<entity>` view represents the corresponding CSV business grain documented in [data_contracts.md](data_contracts.md). Identifiers stay strings. Text is trimmed and blank values become null; documented categories are normalized to a consistent case. Date, boolean, integer, and decimal text is safely cast to native Snowflake types. The models retain `ingested_at`, `source_file`, `batch_id`, `source_system`, and `row_hash` for lineage.

The RAW loader merges on business keys, but Snowflake standard-table keys are informational. Staging selects the latest row per business key by ingestion timestamp and stable metadata tie-breakers. Tests check staging uniqueness and detect unexpected duplicate RAW keys so deduplication cannot silently hide a source defect.

Every structurally valid RAW row is eligible for staging, including rows classified `WARNING` or `QUARANTINED`. Models expose `quality_status`, the `quality_issues` array, and `is_quarantined`. Known issues therefore remain available for downstream policy decisions. `RAW.INGESTION_QUARANTINE` is an audit sidecar with per-attempt payloads; it is not joined into entity views because repeated attempts could multiply rows. A null produced by an unexpected safe-cast failure is a test failure to investigate, not a reason to drop the row.

## Freshness and validation

Each of the nine RAW sources uses `_INGESTED_AT` for freshness. The DEV dataset is manually refreshed: freshness warns after 14 days and errors after 30 days without a source-row load. An unchanged ingestion rerun updates batch audits but leaves current source rows' `_INGESTED_AT` unchanged, so freshness measures changed or newly loaded entity data, not loader invocation frequency.

After exporting the required `SNOWFLAKE_*` connection values into the shell and creating the local profile, run from the repository root:

```powershell
.\.venv\Scripts\dbt.exe parse --project-dir dbt --profiles-dir dbt
.\.venv\Scripts\dbt.exe compile --project-dir dbt --profiles-dir dbt --select tag:staging
.\.venv\Scripts\dbt.exe source freshness --project-dir dbt --profiles-dir dbt
.\.venv\Scripts\dbt.exe run --project-dir dbt --profiles-dir dbt --select tag:staging
.\.venv\Scripts\dbt.exe test --project-dir dbt --profiles-dir dbt --select tag:staging --indirect-selection buildable
```

The source and model YAML under `dbt/models/staging/` describe grains, keys, conversions, quality fields, and column-level tests. Reconcile staged counts, keys, and quality classifications with RAW after each load. Freshness results should be interpreted against the manual DEV refresh schedule; an alert calls for checking source-row load times and the ingestion audits.
