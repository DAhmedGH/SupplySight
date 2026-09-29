# Snowflake RAW ingestion

## Scope

The RAW loader transfers the nine CSV contracts defined in [data_contracts.md](data_contracts.md) into the `SUPPLY_CHAIN_DEV.RAW` schema. It is a Python ingestion workflow; it does not transform source values, run dbt, or create downstream models. The destination database and warehouse are fixed to `SUPPLY_CHAIN_DEV` and `SUPPLY_CHAIN_DEV_WH`. Credentials are supplied through environment configuration and are never stored in source control.

The source files are a single related entity graph. The loader validates the complete graph before opening a Snowflake session so malformed or referentially invalid input cannot leave a partially loaded graph.

## Source and RAW contracts

The nine destination tables are `PRODUCTS`, `WAREHOUSES`, `SUPPLIERS`, `CUSTOMERS`, `ORDERS`, `INVENTORY_SNAPSHOTS`, `PURCHASE_ORDERS`, `SHIPMENTS`, and `RETURNS`. Their source columns and grains match the CSV contracts. Source values are stored as `VARCHAR` to preserve their original CSV representation, including blank optional values. dbt staging converts source values to analytical types.

Each source table adds the following ingestion columns:

| Column | Meaning |
| --- | --- |
| `_INGESTED_AT` | Timestamp at which the row was processed by the loader. |
| `_SOURCE_FILE` | Source CSV basename. |
| `_BATCH_ID` | UUID identifying one loader invocation. |
| `_SOURCE_SYSTEM` | Constant identifier for the synthetic SupplySight source. |
| `_ROW_HASH` | Deterministic hash of the source values, used to detect changed rows. |
| `_QUALITY_STATUS` | `CLEAN`, `WARNING`, or `QUARANTINED`. |
| `_QUALITY_ISSUES` | Array of stable quality issue codes; empty for clean rows. |

`INGESTION_BATCHES` records invocation status and aggregate counts. `INGESTION_BATCH_FILES` records each source file's SHA-256, size, status, and row counts. `INGESTION_QUARANTINE` stores the source payload, business key, issue codes, and batch lineage for rows classified as quarantined. The DDL is maintained in [001_raw_tables.sql](../sql/raw/001_raw_tables.sql). Business keys document source grain; Snowflake standard-table primary key declarations are informational and do not enforce uniqueness.

## Relationship and validation behavior

The loader checks all nine expected files, exact headers, CSV shape and encoding, required/type/domain rules, unique business keys, and declared foreign-key relationships before database mutation. A missing file, malformed CSV, invalid required value, duplicate key, or unresolved relationship is a hard preflight failure. Such a failure aborts the batch before any RAW source rows are merged; rejected structural records are written as JSONL under the configured local quarantine directory for review. This local sidecar is separate from the Snowflake `INGESTION_QUARANTINE` table, which contains structurally valid rows classified with soft quality issues.

Numeric source fields must use plain decimal notation representable within `NUMBER(38,18)`: at most 20 significant integer digits and 18 fractional digits. Exponent notation and values beyond that precision fail preflight. Accepted source text is still stored unchanged in RAW.

Documented business quality findings preserve their source values:

| Finding | RAW status | Handling |
| --- | --- | --- |
| Missing product subcategory | `WARNING` | Load the source row and issue code. |
| Nonstandard customer region casing | `WARNING` | Load the original casing and issue code. |
| Delivered shipment with blank delivery date | `QUARANTINED` | Load into RAW and copy the payload to `INGESTION_QUARANTINE`. |
| Discount outside the inclusive 0–1 range | `QUARANTINED` | Load into RAW and copy the payload to `INGESTION_QUARANTINE`. |
| Inventory availability does not equal on-hand less reserved | `QUARANTINED` | Load into RAW and copy the payload to `INGESTION_QUARANTINE`. |

Quality quarantine is a sidecar classification, not deletion: every structurally valid source row remains represented in its RAW entity table, preserving valid foreign-key relationships. A batch can finish with `SUCCEEDED` status while containing warning or quarantined rows; inspect its quality counts and the quarantine records separately. Batch and file audit counts distinguish source rows, merged rows, inserts, updates, unchanged rows, warning rows, quarantined rows, and quarantine records.

## Batch and rerun behavior

Each invocation receives a UUID batch ID. The loader records a SHA-256 digest and byte size for each input file. Within each entity, it merges rows on the source business key documented in the data contract. A new key inserts a row. A matching key is unchanged only when its source hash and quality classification are unchanged; a change to either updates the current RAW row. The workflow does not delete target rows when a source file omits them.

Reprocessing identical files is idempotent for source values: the second run makes no source-row inserts or updates. The new invocation still receives its own batch audit record. Quarantined payloads are recorded with batch lineage for each attempt. Changed source rows update the current representation and carry the latest ingestion metadata. Historical source versions are not retained in these current-state RAW tables; batch and file audit records provide load history.

## Configuration and operation

Set the Snowflake connection through the `SNOWFLAKE_*` environment variables documented in the repository README and local `.env.example`. Use only the `SUPPLY_CHAIN_DEV` database, `RAW` schema, and `SUPPLY_CHAIN_DEV_WH` warehouse. The configured role must have only the development permissions needed to create/use the RAW tables and write audit and quarantine records.

Generate or select the nine CSV files under `data/generated/`. Run local preflight independently to review source counts and soft quality findings without opening a Snowflake connection:

```powershell
python -c "from supplysight.ingestion import preflight_dataset; r = preflight_dataset('data/generated/dev-2024'); print(r.row_counts); print(r.warning_counts)"
```

After successful preflight, run the live DEV loader:

```powershell
python scripts/ingest_raw.py --input-dir data/generated/dev-2024 --env-file .env --quarantine-dir data/quarantine
```

The loader completes local preflight before connecting. After verifying the active user, role, database, schema, and warehouse, it runs the approved fixed-target DDL in `sql/raw/001_raw_tables.sql` to create any missing RAW source, audit, and quarantine tables. It then checks existing table columns and types for compatibility before loading. A mismatch stops the run before row merges. The Snowflake role therefore needs permission to create the required objects in `SUPPLY_CHAIN_DEV.RAW` as well as write to them. `data/quarantine/` contains local structural rejects and is excluded from version control.

Review the batch and file audit records after each load. Confirm all nine files completed, source and merged row counts reconcile, quality counts match the local preflight report, and the target's per-entity key counts agree with the expected current dataset. A repeated load of unchanged input should report unchanged rows and no source-row updates.

The loader ends at Snowflake RAW. dbt transformations, forecasting, and Power BI are documented in their respective runbooks.
