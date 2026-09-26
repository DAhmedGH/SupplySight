{{ config(materialized='table') }}
select md5(o.order_line_id) as order_fact_key, o.order_line_id, o.order_id,
       d.date_key as order_date_key, c.customer_key, w.warehouse_key, o.selected_product_key as product_key,
       o.product_history_fallback,
       o.quantity, o.unit_price, o.discount_pct,
       o.quantity * o.unit_price * (1 - coalesce(o.discount_pct, 0)) as net_order_amount,
       o.status, o.promised_delivery_date, o.quality_status, o.quality_issues,
       o.ingested_at, o.source_file, o.batch_id, o.source_system, o.row_hash
from {{ ref('int_order_eligibility') }} o
join {{ ref('dim_customer') }} c on o.customer_id = c.customer_id
join {{ ref('dim_warehouse') }} w on o.warehouse_id = w.warehouse_id
join {{ ref('dim_date') }} d on o.order_date = d.date_day
where o.eligible_for_fact
