{{ config(tags=['marts']) }}

with expected as (
    select 'orders' as source_entity, count_if(product_history_fallback) as fallback_count
    from {{ ref('fact_orders') }}
    union all select 'inventory', count_if(product_history_fallback)
    from {{ ref('fact_inventory_snapshot') }}
    union all select 'purchase_orders', count_if(product_history_fallback)
    from {{ ref('fact_purchase_orders') }}
    union all select 'shipments', count_if(product_history_fallback)
    from {{ ref('fact_shipments') }}
    union all select 'returns', count_if(product_history_fallback)
    from {{ ref('fact_returns') }}
), actual as (
    select 'orders' as source_entity, sum(product_history_fallback_line_count) as fallback_count
    from {{ ref('sales_monthly') }}
    union all select 'inventory', sum(product_history_fallback_snapshot_count)
    from {{ ref('inventory_monthly') }}
    union all select 'purchase_orders', sum(product_history_fallback_po_line_count)
    from {{ ref('supplier_monthly') }}
    union all select 'shipments', sum(product_history_fallback_shipment_count)
    from {{ ref('logistics_monthly') }}
    union all select 'returns', sum(product_history_fallback_return_count)
    from {{ ref('return_monthly') }}
)
select e.source_entity, e.fallback_count as core_count, a.fallback_count as mart_count
from expected e
join actual a on e.source_entity = a.source_entity
where e.fallback_count is distinct from a.fallback_count
