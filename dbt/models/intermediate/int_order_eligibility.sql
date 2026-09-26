{{ config(materialized='view') }}
select o.*, pc.selected_product_key, pc.selected_product_quality_status, pc.product_history_fallback,
       (o.quality_status <> 'QUARANTINED'
        and o.order_line_id is not null and o.order_date is not null
        and o.quantity is not null and o.unit_price is not null
        and c.customer_id is not null and w.warehouse_id is not null and d.date_day is not null
        and pc.selected_product_key is not null and coalesce(pc.selected_product_quality_status <> 'QUARANTINED', false)) as eligible_for_fact,
       case when o.quality_status = 'QUARANTINED' then 'QUARANTINED_SOURCE_ROW'
            when o.order_date is null or o.quantity is null or o.unit_price is null then 'INVALID_REQUIRED_MEASURE_OR_DATE'
            when c.customer_id is null then 'MISSING_CUSTOMER_DIMENSION'
            when w.warehouse_id is null then 'MISSING_WAREHOUSE_DIMENSION'
            when d.date_day is null then 'DATE_OUTSIDE_DIM_DATE'
            when pc.selected_product_key is null then 'MISSING_PRODUCT_DIMENSION'
            when pc.selected_product_quality_status = 'QUARANTINED' then 'QUARANTINED_PRODUCT_DIMENSION'
       end as exclusion_reason
from {{ ref('stg_orders') }} o
left join {{ ref('dim_customer') }} c on o.customer_id = c.customer_id
left join {{ ref('dim_warehouse') }} w on o.warehouse_id = w.warehouse_id
left join {{ ref('dim_date') }} d on o.order_date = d.date_day
left join {{ ref('int_fact_product_context') }} pc on pc.source_entity = 'orders' and pc.source_key = o.order_line_id
