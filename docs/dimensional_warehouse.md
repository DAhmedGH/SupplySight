# Dimensional warehouse

## Scope and location

The dimensional warehouse reads the nine typed staging views in `SUPPLY_CHAIN_DEV.STAGING`. dbt builds the required eligibility and exception views in `SUPPLY_CHAIN_DEV.INTERMEDIATE`, records product versions with a snapshot, and materializes conformed dimensions and facts in `SUPPLY_CHAIN_DEV.CORE`. All operations use `SUPPLY_CHAIN_DEV_WH`. No business marts are part of this layer.

The five fact grains are:

| Fact | Grain | Source business key |
| --- | --- | --- |
| `fact_orders` | One order line | `order_line_id` |
| `fact_inventory_snapshot` | One product, warehouse, and snapshot date | `snapshot_date`, `warehouse_id`, `product_id` |
| `fact_purchase_orders` | One purchase order line | `po_line_id` |
| `fact_shipments` | One shipment | `shipment_id` |
| `fact_returns` | One return | `return_id` |

`dim_date` has one row per calendar date. Supplier, warehouse, and customer dimensions have one row per source business key. `dim_carrier` has one row per distinct normalized carrier in shipment data; it is source-derived because there is no carrier master contract. `dim_product` has one row per observed product version. Fact foreign keys use warehouse surrogate keys, while source identifiers remain available as degenerate keys for reconciliation and traceability.

## Product history

The product snapshot uses dbt's check strategy over product attributes and quality classification. It captures a new version when an observed product row changes. Its `dbt_valid_from` and `dbt_valid_to` timestamps describe when the warehouse observed those versions, not when product attributes changed in the source system. The Phase 3 RAW tables keep only current rows, so attributes before the first snapshot cannot be reconstructed.

Facts select the product version observed at the end of their event date. Source events have dates rather than timestamps, so within-day ordering of a product change and an event cannot be recovered. For an event before the first observed version, facts use that product's earliest known version and mark `product_history_fallback`. This baseline allows historical 2024 events to retain a product foreign key without presenting the snapshot's validity timestamp as a 2024 effective date. A later product change creates a distinct dimension version for subsequent event dates.

The initial DEV dataset predates the first product snapshot. Consequently, every current fact uses the earliest-known-version fallback. Future snapshots preserve warehouse-observed changes, but the existing inputs cannot recover product attributes as they stood during 2024.

## Quality policy

WARNING rows remain eligible after staging normalization and carry their quality status, issues, and ingestion lineage into the warehouse. QUARANTINED rows are retained in staging but excluded from conformed fact rows when they represent known invalid measures or event attributes. Intermediate eligibility views retain every staged fact-grain row, mark the inclusion decision, and record an exclusion reason. The unioned quality-exceptions view provides one auditable row for each excluded fact-grain record. Tests reconcile eligible fact counts and excluded counts to staging, so exclusions cannot disappear silently.

The RAW quarantine sidecar records every ingestion attempt and is not joined into facts; joining it would multiply current records after repeated loads. Product dimension versions retain their quality classification, including quarantined versions, so observed SCD intervals remain auditable. Eligible facts must resolve to a non-quarantined product version; facts whose selected event-time version is quarantined belong in the exceptions output. Tests require fact surrogate keys to resolve to a valid dimension row. Optional dates remain nullable where the source contract allows them.

Shipments and returns use order lines for dimensional context. A quarantined order discount does not by itself invalidate a shipment or return event: these facts retain eligible event rows and expose the related order's quality classification so that the inherited concern remains visible. The current DEV data includes shipments and returns linked to quarantined orders, so this rule matters for row-count reconciliation.

## Build and validation

Use the local ignored dbt profile documented in [staging.md](staging.md). The profile fixes the database and warehouse to `SUPPLY_CHAIN_DEV` and `SUPPLY_CHAIN_DEV_WH`. The project schema routing places staging, intermediate, and core relations in the corresponding DEV schemas.

Run the product snapshot before the dimensional build. From the repository root, with the documented `SNOWFLAKE_*` connection values exported:

```powershell
.\.venv\Scripts\dbt.exe parse --project-dir dbt --profiles-dir dbt
.\.venv\Scripts\dbt.exe compile --project-dir dbt --profiles-dir dbt
.\.venv\Scripts\dbt.exe snapshot --project-dir dbt --profiles-dir dbt --select snap_products
.\.venv\Scripts\dbt.exe run --project-dir dbt --profiles-dir dbt --select tag:intermediate tag:core
.\.venv\Scripts\dbt.exe test --project-dir dbt --profiles-dir dbt
```

Check the documented key and relationship tests, product-version validity, and the staging-to-fact-plus-exception reconciliation after each build. Re-run the product snapshot before refreshing CORE when product attributes may have changed.
