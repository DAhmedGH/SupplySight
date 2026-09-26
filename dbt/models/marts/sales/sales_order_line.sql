{{ config(materialized='table') }}

with return_context as (
    select order_line_id,
           count(*) as return_count,
           sum(returned_quantity) as returned_quantity,
           sum(refund_amount) as refund_amount,
           max(iff(quality_status = 'WARNING', 1, 0)) = 1 as has_warning_return,
           max(iff(upstream_order_quality_status = 'QUARANTINED', 1, 0)) = 1 as has_quarantined_order_context,
           max(iff(product_history_fallback, 1, 0)) = 1 as has_return_product_history_fallback
    from {{ ref('fact_returns') }}
    group by order_line_id
)
select
    o.order_fact_key,
    o.order_line_id,
    o.order_id,
    o.order_date_key,
    o.customer_key,
    o.product_key,
    o.warehouse_key,
    o.product_history_fallback,
    o.quantity,
    o.unit_price,
    o.discount_pct,
    o.status as order_status,
    o.net_order_amount as booked_order_amount,
    p.unit_cost,
    case when o.status = 'delivered' then o.net_order_amount else 0 end as revenue_amount,
    case when o.status = 'delivered' then o.quantity * p.unit_cost else 0 end as cost_of_goods_sold,
    case when o.status = 'delivered' then o.net_order_amount - (o.quantity * p.unit_cost) else 0 end as gross_profit_amount,
    case when o.status = 'delivered'
         then (o.net_order_amount - (o.quantity * p.unit_cost)) / nullif(o.net_order_amount, 0)
    end as gross_margin_rate,
    (o.status = 'delivered') as is_revenue_recognized,
    coalesce(r.return_count, 0) as linked_return_count,
    coalesce(r.returned_quantity, 0) as linked_returned_quantity,
    coalesce(r.refund_amount, 0) as linked_refund_amount,
    coalesce(r.has_warning_return, false) as has_warning_linked_return,
    coalesce(r.has_quarantined_order_context, false) as has_quarantined_linked_return_context,
    coalesce(r.has_return_product_history_fallback, false) as has_linked_return_product_history_fallback,
    o.quality_status,
    o.quality_issues,
    o.batch_id,
    o.source_file,
    o.source_system,
    o.row_hash,
    o.ingested_at
from {{ ref('fact_orders') }} o
join {{ ref('dim_product') }} p on o.product_key = p.product_key
left join return_context r on o.order_line_id = r.order_line_id
