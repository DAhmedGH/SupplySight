{{ config(tags=['marts']) }}

with expected as (
    select 'order_line_count' as metric, count(*)::number(38, 6) as value
    from {{ ref('fact_orders') }}
    union all select 'recognized_revenue', coalesce(sum(net_order_amount), 0)
    from {{ ref('fact_orders') }} where status = 'delivered'
    union all select 'recognized_cogs', coalesce(sum(o.quantity * p.unit_cost), 0)
    from {{ ref('fact_orders') }} o
    join {{ ref('dim_product') }} p on o.product_key = p.product_key
    where o.status = 'delivered'
    union all select 'inventory_snapshot_count', count(*)
    from {{ ref('fact_inventory_snapshot') }}
    union all select 'inventory_snapshot_value', coalesce(sum(inventory_value), 0)
    from {{ ref('fact_inventory_snapshot') }}
    union all select 'purchase_order_line_count', count(*)
    from {{ ref('fact_purchase_orders') }}
    union all select 'supplier_spend', coalesce(sum(ordered_cost), 0)
    from {{ ref('fact_purchase_orders') }}
    union all select 'shipment_count', count(*)
    from {{ ref('fact_shipments') }}
    union all select 'shipped_quantity', coalesce(sum(shipped_quantity), 0)
    from {{ ref('fact_shipments') }}
    union all select 'return_count', count(*)
    from {{ ref('fact_returns') }}
    union all select 'refund_amount', coalesce(sum(refund_amount), 0)
    from {{ ref('fact_returns') }}
    union all select 'excluded_record_count', count(*)
    from {{ ref('int_fact_quality_exceptions') }}
), actual as (
    select 'order_line_count' as metric, count(*)::number(38, 6) as value
    from {{ ref('sales_order_line') }}
    union all select 'recognized_revenue', coalesce(sum(revenue_amount), 0)
    from {{ ref('sales_order_line') }}
    union all select 'recognized_cogs', coalesce(sum(cost_of_goods_sold), 0)
    from {{ ref('sales_order_line') }}
    union all select 'inventory_snapshot_count', count(*)
    from {{ ref('inventory_detail') }}
    union all select 'inventory_snapshot_value', coalesce(sum(inventory_value), 0)
    from {{ ref('inventory_detail') }}
    union all select 'purchase_order_line_count', count(*)
    from {{ ref('supplier_detail') }}
    union all select 'supplier_spend', coalesce(sum(supplier_spend), 0)
    from {{ ref('supplier_detail') }}
    union all select 'shipment_count', count(*)
    from {{ ref('shipment_detail') }}
    union all select 'shipped_quantity', coalesce(sum(shipped_quantity), 0)
    from {{ ref('shipment_detail') }}
    union all select 'return_count', coalesce(sum(return_count), 0)
    from {{ ref('return_monthly') }}
    union all select 'refund_amount', coalesce(sum(refund_amount), 0)
    from {{ ref('return_monthly') }}
    union all select 'excluded_record_count', count(*)
    from {{ ref('mart_quality_exceptions') }}
)
select coalesce(e.metric, a.metric) as metric, e.value as core_value, a.value as mart_value
from expected e
full join actual a on e.metric = a.metric
where e.metric is null or a.metric is null or abs(e.value - a.value) > 0.0001
