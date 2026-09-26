{{ config(tags=['marts']) }}

with sales_by_day as (
    select p.product_id, o.warehouse_key, d.date_day,
           sum(o.quantity) as delivered_units,
           sum(o.quantity * p.unit_cost) as delivered_cogs
    from {{ ref('fact_orders') }} o
    join {{ ref('dim_product') }} p on o.product_key = p.product_key
    join {{ ref('dim_date') }} d on o.order_date_key = d.date_key
    where o.status = 'delivered'
    group by p.product_id, o.warehouse_key, d.date_day
), expected as (
    select m.inventory_monthly_key,
           w.warehouse_key as expected_warehouse_key,
           coalesce(sum(s.delivered_units), 0) as delivered_units,
           coalesce(sum(s.delivered_cogs), 0) as delivered_cogs
    from {{ ref('inventory_monthly') }} m
    join {{ ref('dim_warehouse') }} w on m.warehouse_id = w.warehouse_id
    left join sales_by_day s on m.product_id = s.product_id
        and w.warehouse_key = s.warehouse_key
        and s.date_day > dateadd(day, -90, last_day(m.month_start))
        and s.date_day <= last_day(m.month_start)
    group by m.inventory_monthly_key, w.warehouse_key
)
select m.inventory_monthly_key, m.product_id, m.warehouse_id,
       e.delivered_units as core_units, m.demand_units_trailing_90d as mart_units,
       e.delivered_cogs as core_cogs, m.sales_cogs_trailing_90d as mart_cogs
from {{ ref('inventory_monthly') }} m
left join expected e on m.inventory_monthly_key = e.inventory_monthly_key
where e.inventory_monthly_key is null
   or m.warehouse_key is distinct from e.expected_warehouse_key
   or abs(e.delivered_units - m.demand_units_trailing_90d) > 0.0001
   or abs(e.delivered_cogs - m.sales_cogs_trailing_90d) > 0.0001
