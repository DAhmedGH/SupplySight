with sales as (
    select order_month as month_start,
           sum(order_line_count) as order_line_count,
           sum(delivered_order_line_count) as delivered_order_line_count,
           sum(cancelled_order_line_count) as cancelled_order_line_count,
           sum(ordered_quantity) as ordered_quantity,
           sum(delivered_quantity) as delivered_quantity,
           sum(booked_order_amount) as booked_order_amount,
           sum(revenue_amount) as revenue_amount,
           sum(cost_of_goods_sold) as cost_of_goods_sold,
           sum(gross_profit_amount) as gross_profit_amount,
           sum(warning_order_line_count) as warning_order_line_count,
           sum(product_history_fallback_line_count) as fallback_order_line_count
    from {{ ref('sales_monthly') }}
    group by order_month
), returns as (
    select return_month as month_start,
           sum(return_count) as return_count,
           sum(returned_quantity) as returned_quantity,
           sum(refund_amount) as refund_amount,
           sum(warning_return_count) as warning_return_count,
           sum(upstream_quarantined_order_count) as upstream_quarantined_return_count,
           sum(product_history_fallback_return_count) as fallback_return_count
    from {{ ref('return_monthly') }}
    group by return_month
), inventory as (
    select month_start,
           sum(snapshot_count) as inventory_snapshot_count,
           sum(stockout_snapshot_count) as stockout_snapshot_count,
           sum(avg_inventory_value) as average_inventory_value,
           sum(month_end_inventory_value) as month_end_inventory_value,
           sum(avg_available_units) as average_available_units,
           sum(month_end_excess_units) as excess_inventory_units,
           sum(month_end_excess_value) as excess_inventory_value,
           sum(dead_stock_units) as dead_stock_units,
           sum(dead_stock_value) as dead_stock_value,
           sum(warning_snapshot_count) as warning_snapshot_count,
           sum(product_history_fallback_snapshot_count) as fallback_inventory_snapshot_count
    from {{ ref('inventory_monthly') }}
    group by month_start
), supplier as (
    select month_start,
           sum(supplier_spend) as supplier_spend,
           sum(purchase_order_line_count) as purchase_order_line_count,
           sum(otif_numerator) as supplier_otif_numerator,
           sum(otif_denominator) as supplier_otif_denominator,
           sum(fill_rate_numerator) as supplier_fill_numerator,
           sum(fill_rate_denominator) as supplier_fill_denominator,
           sum(late_purchase_order_count) as late_purchase_order_count,
           sum(due_purchase_order_count) as due_purchase_order_count,
           sum(lead_time_numerator_days) as lead_time_days_sum,
           sum(lead_time_denominator_count) as lead_time_observation_count,
           sum(warning_po_line_count) as warning_po_line_count,
           sum(product_history_fallback_po_line_count) as fallback_po_line_count
    from {{ ref('supplier_monthly') }}
    group by month_start
), logistics as (
    select ship_month as month_start,
           sum(shipment_count) as shipment_count,
           sum(shipped_quantity) as shipped_quantity,
           sum(completed_shipment_count) as completed_shipment_count,
           sum(on_time_shipment_count) as on_time_shipment_count,
           sum(timeliness_eligible_count) as timeliness_eligible_count,
           sum(late_shipment_count) as late_shipment_count,
           sum(warning_shipment_count) as warning_shipment_count,
           sum(upstream_quarantined_order_count) as upstream_quarantined_shipment_count,
           sum(product_history_fallback_shipment_count) as fallback_shipment_count
    from {{ ref('logistics_monthly') }}
    group by ship_month
), exceptions as (
    select ingestion_month as month_start,
           sum(excluded_record_count) as excluded_record_count,
           sum(quarantined_record_count) as quarantined_record_count
    from {{ ref('mart_quality_exceptions_monthly') }}
    group by ingestion_month
), months as (
    select month_start from sales union
    select month_start from returns union
    select month_start from inventory union
    select month_start from supplier union
    select month_start from logistics union
    select month_start from exceptions
)
select m.month_start,
       coalesce(s.order_line_count, 0) as order_line_count,
       coalesce(s.delivered_order_line_count, 0) as delivered_order_line_count,
       coalesce(s.cancelled_order_line_count, 0) as cancelled_order_line_count,
       coalesce(s.ordered_quantity, 0) as ordered_quantity,
       coalesce(s.delivered_quantity, 0) as delivered_quantity,
       coalesce(s.booked_order_amount, 0) as booked_order_amount,
       coalesce(s.revenue_amount, 0) as revenue_amount,
       coalesce(s.cost_of_goods_sold, 0) as cost_of_goods_sold,
       coalesce(s.gross_profit_amount, 0) as gross_profit_amount,
       s.gross_profit_amount / nullif(s.revenue_amount, 0) as gross_margin_rate,
       coalesce(r.return_count, 0) as return_count,
       coalesce(r.returned_quantity, 0) as returned_quantity,
       coalesce(r.refund_amount, 0) as refund_amount,
       coalesce(i.inventory_snapshot_count, 0) as inventory_snapshot_count,
       coalesce(i.stockout_snapshot_count, 0) as stockout_snapshot_count,
       i.stockout_snapshot_count / nullif(i.inventory_snapshot_count, 0) as stockout_rate,
       i.average_inventory_value,
       i.month_end_inventory_value,
       i.average_available_units,
       coalesce(i.excess_inventory_units, 0) as excess_inventory_units,
       coalesce(i.excess_inventory_value, 0) as excess_inventory_value,
       coalesce(i.dead_stock_units, 0) as dead_stock_units,
       coalesce(i.dead_stock_value, 0) as dead_stock_value,
       s.cost_of_goods_sold / nullif(i.average_inventory_value, 0) as inventory_turnover_monthly,
       i.average_available_units / nullif(s.delivered_quantity / day(last_day(m.month_start)), 0) as days_of_supply_monthly,
       coalesce(p.supplier_spend, 0) as supplier_spend,
       coalesce(p.purchase_order_line_count, 0) as purchase_order_line_count,
       coalesce(p.supplier_otif_numerator, 0) as supplier_otif_numerator,
       coalesce(p.supplier_otif_denominator, 0) as supplier_otif_denominator,
       p.supplier_otif_numerator / nullif(p.supplier_otif_denominator, 0) as supplier_otif_rate,
       coalesce(p.supplier_fill_numerator, 0) as supplier_fill_numerator,
       coalesce(p.supplier_fill_denominator, 0) as supplier_fill_denominator,
       p.supplier_fill_numerator / nullif(p.supplier_fill_denominator, 0) as supplier_fill_rate,
       coalesce(p.late_purchase_order_count, 0) as late_purchase_order_count,
       coalesce(p.due_purchase_order_count, 0) as due_purchase_order_count,
       coalesce(p.lead_time_days_sum, 0) as lead_time_days_sum,
       coalesce(p.lead_time_observation_count, 0) as lead_time_observation_count,
       p.lead_time_days_sum / nullif(p.lead_time_observation_count, 0) as average_lead_time_days,
       coalesce(l.shipment_count, 0) as shipment_count,
       coalesce(l.shipped_quantity, 0) as shipped_quantity,
       coalesce(l.completed_shipment_count, 0) as completed_shipment_count,
       coalesce(l.on_time_shipment_count, 0) as on_time_shipment_count,
       coalesce(l.timeliness_eligible_count, 0) as timeliness_eligible_count,
       coalesce(l.late_shipment_count, 0) as late_shipment_count,
       l.on_time_shipment_count / nullif(l.timeliness_eligible_count, 0) as on_time_delivery_rate,
       coalesce(e.excluded_record_count, 0) as excluded_record_count_by_ingestion_month,
       coalesce(e.quarantined_record_count, 0) as quarantined_record_count_by_ingestion_month,
       coalesce(s.warning_order_line_count, 0) as warning_order_line_count,
       coalesce(r.warning_return_count, 0) as warning_return_count,
       coalesce(i.warning_snapshot_count, 0) as warning_snapshot_count,
       coalesce(p.warning_po_line_count, 0) as warning_po_line_count,
       coalesce(l.warning_shipment_count, 0) as warning_shipment_count,
       coalesce(l.upstream_quarantined_shipment_count, 0) as upstream_quarantined_shipment_count,
       coalesce(r.upstream_quarantined_return_count, 0) as upstream_quarantined_return_count,
       coalesce(s.fallback_order_line_count, 0) as fallback_order_line_count,
       coalesce(r.fallback_return_count, 0) as fallback_return_count,
       coalesce(i.fallback_inventory_snapshot_count, 0) as fallback_inventory_snapshot_count,
       coalesce(p.fallback_po_line_count, 0) as fallback_po_line_count,
       coalesce(l.fallback_shipment_count, 0) as fallback_shipment_count
from months m
left join sales s on m.month_start = s.month_start
left join returns r on m.month_start = r.month_start
left join inventory i on m.month_start = i.month_start
left join supplier p on m.month_start = p.month_start
left join logistics l on m.month_start = l.month_start
left join exceptions e on m.month_start = e.month_start
