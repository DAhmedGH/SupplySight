{{ config(materialized='view') }}
select x.*, o.order_id, o.customer_id, o.product_id, o.warehouse_id,
       pc.selected_product_key, pc.selected_product_quality_status, pc.product_history_fallback,
       o.quality_status as upstream_order_quality_status,
       o.quality_issues as upstream_order_quality_issues,
       o.batch_id as upstream_order_batch_id,
       (x.quality_status <> 'QUARANTINED' and x.return_id is not null and x.return_date is not null
        and x.quantity is not null and x.refund_amount is not null and o.order_line_id is not null
        and c.customer_id is not null and w.warehouse_id is not null and d.date_day is not null and pc.selected_product_key is not null
        and coalesce(pc.selected_product_quality_status <> 'QUARANTINED', false)) as eligible_for_fact,
       case when x.quality_status = 'QUARANTINED' then 'QUARANTINED_SOURCE_ROW'
            when x.return_date is null or x.quantity is null or x.refund_amount is null then 'INVALID_REQUIRED_MEASURE_OR_DATE'
            when o.order_line_id is null then 'MISSING_ORDER_CONTEXT'
            when c.customer_id is null then 'MISSING_CUSTOMER_DIMENSION'
            when w.warehouse_id is null then 'MISSING_WAREHOUSE_DIMENSION'
            when d.date_day is null then 'DATE_OUTSIDE_DIM_DATE'
            when pc.selected_product_key is null then 'MISSING_PRODUCT_DIMENSION'
            when pc.selected_product_quality_status = 'QUARANTINED' then 'QUARANTINED_PRODUCT_DIMENSION' end as exclusion_reason
from {{ ref('stg_returns') }} x
left join {{ ref('stg_orders') }} o on x.order_line_id = o.order_line_id
left join {{ ref('dim_customer') }} c on o.customer_id = c.customer_id
left join {{ ref('dim_warehouse') }} w on o.warehouse_id = w.warehouse_id
left join {{ ref('dim_date') }} d on x.return_date = d.date_day
left join {{ ref('int_fact_product_context') }} pc on pc.source_entity = 'returns' and pc.source_key = x.return_id
