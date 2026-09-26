{{ config(materialized='table') }}

select
    date_trunc('month', to_date(to_varchar(s.order_date_key), 'YYYYMMDD'))::date as order_month,
    s.customer_key,
    s.product_key,
    s.warehouse_key,
    count(*) as order_line_count,
    sum(s.quantity) as ordered_quantity,
    sum(case when s.order_status = 'delivered' then s.quantity else 0 end) as delivered_quantity,
    sum(s.booked_order_amount) as booked_order_amount,
    count_if(s.order_status = 'delivered') as delivered_order_line_count,
    count_if(s.order_status = 'processing') as processing_order_line_count,
    count_if(s.order_status = 'backordered') as backordered_order_line_count,
    count_if(s.order_status = 'partially_fulfilled') as partially_fulfilled_order_line_count,
    count_if(s.order_status = 'cancelled') as cancelled_order_line_count,
    count_if(s.is_revenue_recognized) as revenue_recognized_order_line_count,
    sum(s.revenue_amount) as revenue_amount,
    sum(s.cost_of_goods_sold) as cost_of_goods_sold,
    sum(s.gross_profit_amount) as gross_profit_amount,
    sum(s.gross_profit_amount) / nullif(sum(s.revenue_amount), 0) as gross_margin_rate,
    sum(s.linked_return_count) as linked_return_count,
    sum(s.linked_returned_quantity) as linked_returned_quantity,
    sum(s.linked_refund_amount) as linked_refund_amount,
    count_if(s.quality_status = 'WARNING') as warning_order_line_count,
    count_if(s.product_history_fallback) as product_history_fallback_line_count,
    count_if(s.has_warning_linked_return) as order_lines_with_warning_linked_returns,
    count_if(s.has_quarantined_linked_return_context) as order_lines_with_quarantined_linked_return_context
from {{ ref('sales_order_line') }} s
group by 1, 2, 3, 4
