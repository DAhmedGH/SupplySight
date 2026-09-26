{{ config(materialized='table') }}
select md5(concat_ws('|', i.snapshot_date, i.warehouse_id, i.product_id)) as inventory_snapshot_key,
       d.date_key as snapshot_date_key, w.warehouse_key, i.selected_product_key as product_key,
       i.product_history_fallback,
       i.snapshot_date, i.warehouse_id, i.product_id,
       i.on_hand_units, i.reserved_units, i.available_units, i.in_transit_units, i.inventory_value,
       i.quality_status, i.quality_issues, i.ingested_at, i.source_file, i.batch_id,
       i.source_system, i.row_hash
from {{ ref('int_inventory_eligibility') }} i
join {{ ref('dim_date') }} d on i.snapshot_date = d.date_day
join {{ ref('dim_warehouse') }} w on i.warehouse_id = w.warehouse_id
where i.eligible_for_fact
