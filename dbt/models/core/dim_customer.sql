{{ config(materialized='table') }}
select md5(customer_id) as customer_key, customer_id, customer_segment, region, country,
       signup_date, quality_status, quality_issues, ingested_at, source_file, batch_id,
       source_system, row_hash
from {{ ref('stg_customers') }} where quality_status <> 'QUARANTINED'
