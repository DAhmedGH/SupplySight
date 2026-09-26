{{ config(materialized='view') }}
select x.*, pc.selected_product_key, pc.selected_product_quality_status, pc.product_history_fallback,
       (x.quality_status <> 'QUARANTINED' and x.po_line_id is not null
        and x.order_date is not null and x.ordered_quantity is not null and x.unit_cost is not null
        and s.supplier_id is not null and w.warehouse_id is not null and d.date_day is not null and pc.selected_product_key is not null
        and coalesce(pc.selected_product_quality_status <> 'QUARANTINED', false)) as eligible_for_fact,
       case when x.quality_status = 'QUARANTINED' then 'QUARANTINED_SOURCE_ROW'
            when x.order_date is null or x.ordered_quantity is null or x.unit_cost is null then 'INVALID_REQUIRED_MEASURE_OR_DATE'
            when s.supplier_id is null then 'MISSING_SUPPLIER_DIMENSION'
            when w.warehouse_id is null then 'MISSING_WAREHOUSE_DIMENSION'
            when d.date_day is null then 'DATE_OUTSIDE_DIM_DATE'
            when pc.selected_product_key is null then 'MISSING_PRODUCT_DIMENSION'
            when pc.selected_product_quality_status = 'QUARANTINED' then 'QUARANTINED_PRODUCT_DIMENSION' end as exclusion_reason
from {{ ref('stg_purchase_orders') }} x
left join {{ ref('dim_supplier') }} s on x.supplier_id = s.supplier_id
left join {{ ref('dim_warehouse') }} w on x.warehouse_id = w.warehouse_id
left join {{ ref('dim_date') }} d on x.order_date = d.date_day
left join {{ ref('int_fact_product_context') }} pc on pc.source_entity = 'purchase_orders' and pc.source_key = x.po_line_id
