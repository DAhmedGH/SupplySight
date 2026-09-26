{{ config(materialized='view') }}
select i.*, md5(concat_ws('|', i.snapshot_date, i.warehouse_id, i.product_id)) as inventory_snapshot_key,
       pc.selected_product_key, pc.selected_product_quality_status, pc.product_history_fallback,
       (i.quality_status <> 'QUARANTINED' and i.snapshot_date is not null
        and i.on_hand_units is not null and i.reserved_units is not null
        and i.available_units is not null and i.in_transit_units is not null and i.inventory_value is not null
        and w.warehouse_id is not null and d.date_day is not null and pc.selected_product_key is not null
        and coalesce(pc.selected_product_quality_status <> 'QUARANTINED', false)) as eligible_for_fact,
       case when i.quality_status = 'QUARANTINED' then 'QUARANTINED_SOURCE_ROW'
            when i.snapshot_date is null or i.on_hand_units is null or i.reserved_units is null
              or i.available_units is null or i.in_transit_units is null or i.inventory_value is null then 'INVALID_REQUIRED_MEASURE_OR_DATE'
            when w.warehouse_id is null then 'MISSING_WAREHOUSE_DIMENSION'
            when d.date_day is null then 'DATE_OUTSIDE_DIM_DATE'
            when pc.selected_product_key is null then 'MISSING_PRODUCT_DIMENSION'
            when pc.selected_product_quality_status = 'QUARANTINED' then 'QUARANTINED_PRODUCT_DIMENSION'
       end as exclusion_reason
from {{ ref('stg_inventory_snapshots') }} i
left join {{ ref('dim_warehouse') }} w on i.warehouse_id = w.warehouse_id
left join {{ ref('dim_date') }} d on i.snapshot_date = d.date_day
left join {{ ref('int_fact_product_context') }} pc on pc.source_entity = 'inventory_snapshots'
  and pc.source_key = concat_ws('|', i.snapshot_date, i.warehouse_id, i.product_id)
