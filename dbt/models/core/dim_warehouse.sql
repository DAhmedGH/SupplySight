{{ config(materialized='table') }}
select md5(warehouse_id) as warehouse_key, warehouse_id, warehouse_name, region, country,
       capacity_units, quality_status, quality_issues, ingested_at, source_file, batch_id,
       source_system, row_hash
from {{ ref('stg_warehouses') }} where quality_status <> 'QUARANTINED'
