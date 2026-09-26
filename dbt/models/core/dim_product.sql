{{ config(materialized='table') }}
select dbt_scd_id as product_key,
       product_id, sku, product_name, category, subcategory, unit_cost, list_price,
       launch_date, active_flag, reorder_point,
       dbt_valid_from as valid_from, dbt_valid_to as valid_to,
       dbt_valid_to is null as is_current,
       quality_status, quality_issues, is_quarantined,
       ingested_at, source_file, batch_id, source_system, row_hash
from {{ ref('snap_products') }}
