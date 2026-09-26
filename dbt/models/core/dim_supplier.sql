{{ config(materialized='table') }}
select md5(supplier_id) as supplier_key, supplier_id, supplier_name, region, country,
       lead_time_days, lead_time_std_days, on_time_rate, quality_rate, cost_factor,
       active_flag, quality_status, quality_issues, ingested_at, source_file, batch_id,
       source_system, row_hash
from {{ ref('stg_suppliers') }} where quality_status <> 'QUARANTINED'
