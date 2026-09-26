{{ config(tags=['marts']) }}

with expected as (
    select order_month as month_start, 'revenue_amount' as metric, sum(revenue_amount) as value
    from {{ ref('sales_monthly') }} group by order_month
    union all select order_month, 'cost_of_goods_sold', sum(cost_of_goods_sold)
    from {{ ref('sales_monthly') }} group by order_month
    union all select return_month, 'refund_amount', sum(refund_amount)
    from {{ ref('return_monthly') }} group by return_month
    union all select month_start, 'month_end_inventory_value', sum(month_end_inventory_value)
    from {{ ref('inventory_monthly') }} group by month_start
    union all select month_start, 'supplier_spend', sum(supplier_spend)
    from {{ ref('supplier_monthly') }} group by month_start
    union all select month_start, 'supplier_otif_numerator', sum(otif_numerator)
    from {{ ref('supplier_monthly') }} group by month_start
    union all select month_start, 'supplier_otif_denominator', sum(otif_denominator)
    from {{ ref('supplier_monthly') }} group by month_start
    union all select ship_month, 'shipment_count', sum(shipment_count)
    from {{ ref('logistics_monthly') }} group by ship_month
    union all select ingestion_month, 'excluded_record_count_by_ingestion_month', sum(excluded_record_count)
    from {{ ref('mart_quality_exceptions_monthly') }} group by ingestion_month
    union all select order_month, 'fallback_order_line_count', sum(product_history_fallback_line_count)
    from {{ ref('sales_monthly') }} group by order_month
), actual as (
    select month_start, 'revenue_amount' as metric, revenue_amount as value from {{ ref('mart_executive_monthly') }}
    union all select month_start, 'cost_of_goods_sold', cost_of_goods_sold from {{ ref('mart_executive_monthly') }}
    union all select month_start, 'refund_amount', refund_amount from {{ ref('mart_executive_monthly') }}
    union all select month_start, 'month_end_inventory_value', month_end_inventory_value from {{ ref('mart_executive_monthly') }}
    union all select month_start, 'supplier_spend', supplier_spend from {{ ref('mart_executive_monthly') }}
    union all select month_start, 'supplier_otif_numerator', supplier_otif_numerator from {{ ref('mart_executive_monthly') }}
    union all select month_start, 'supplier_otif_denominator', supplier_otif_denominator from {{ ref('mart_executive_monthly') }}
    union all select month_start, 'shipment_count', shipment_count from {{ ref('mart_executive_monthly') }}
    union all select month_start, 'excluded_record_count_by_ingestion_month', excluded_record_count_by_ingestion_month from {{ ref('mart_executive_monthly') }}
    union all select month_start, 'fallback_order_line_count', fallback_order_line_count from {{ ref('mart_executive_monthly') }}
), comparison as (
    select a.month_start, a.metric, coalesce(e.value, 0) as expected_value, a.value as actual_value
    from actual a
    left join expected e on a.month_start = e.month_start and a.metric = e.metric
)
select * from comparison
where abs(coalesce(expected_value, 0) - coalesce(actual_value, 0)) > 0.0001
