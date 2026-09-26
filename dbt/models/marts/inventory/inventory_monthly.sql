{{ config(materialized='table') }}

-- Grain is product business key, warehouse, calendar month. Inventory measures
-- average observed snapshot balance; month-end is the last snapshot in month.
with snapshots as (
    select
        i.*,
        p.sku,
        p.product_name,
        p.category,
        p.reorder_point,
        date_trunc('month', i.snapshot_date)::date as month_start,
        row_number() over (
            partition by p.product_id, i.warehouse_id, date_trunc('month', i.snapshot_date)
            order by i.snapshot_date desc, i.inventory_snapshot_key
        ) as month_end_rank
    from {{ ref('fact_inventory_snapshot') }} i
    join {{ ref('dim_product') }} p on i.product_key = p.product_key
),
monthly_inventory as (
    select
        product_id,
        warehouse_id,
        month_start,
        count(*) as snapshot_count,
        avg(available_units) as avg_available_units,
        avg(inventory_value) as avg_inventory_value,
        sum(iff(available_units <= 0, 1, 0)) as stockout_snapshot_count,
        sum(greatest(available_units - coalesce(reorder_point, 0), 0)) as excess_unit_snapshot_sum,
        count_if(quality_status = 'WARNING') as warning_snapshot_count,
        count_if(product_history_fallback) as product_history_fallback_snapshot_count,
        max(iff(month_end_rank = 1, available_units, null)) as month_end_available_units,
        max(iff(month_end_rank = 1, on_hand_units, null)) as month_end_on_hand_units,
        max(iff(month_end_rank = 1, inventory_value, null)) as month_end_inventory_value,
        max(iff(month_end_rank = 1, reorder_point, null)) as month_end_reorder_point,
        max(iff(month_end_rank = 1, product_key, null)) as month_end_product_key,
        max(iff(month_end_rank = 1, warehouse_key, null)) as month_end_warehouse_key,
        max(iff(month_end_rank = 1, sku, null)) as month_end_sku,
        max(iff(month_end_rank = 1, product_name, null)) as month_end_product_name,
        max(iff(month_end_rank = 1, category, null)) as month_end_category,
        max(iff(month_end_rank = 1, inventory_snapshot_key, null)) as month_end_inventory_snapshot_key,
        max(iff(month_end_rank = 1, quality_status, null)) as month_end_quality_status,
        max(iff(month_end_rank = 1, iff(product_history_fallback, 1, 0), null)) = 1 as month_end_product_history_fallback,
        max(iff(month_end_rank = 1, batch_id, null)) as month_end_batch_id
    from snapshots
    group by product_id, warehouse_id, month_start
),
sales_by_day as (
    select p.product_id, o.warehouse_key, d.date_day as sale_date,
           sum(o.quantity) as demand_units,
           sum(o.quantity * coalesce(p.unit_cost, 0)) as sales_cogs
    from {{ ref('fact_orders') }} o
    join {{ ref('dim_product') }} p on o.product_key = p.product_key
    join {{ ref('dim_date') }} d on o.order_date_key = d.date_key
    where lower(o.status) = 'delivered'
    group by p.product_id, o.warehouse_key, d.date_day
),
trailing_demand as (
    select m.product_id, m.warehouse_id, m.month_start,
           coalesce(sum(s.demand_units), 0) as demand_units_trailing_90d,
           coalesce(sum(s.sales_cogs), 0) as sales_cogs_trailing_90d
    from monthly_inventory m
    left join sales_by_day s on s.product_id = m.product_id
        and s.warehouse_key = m.month_end_warehouse_key
        and s.sale_date > dateadd(day, -90, last_day(m.month_start))
        and s.sale_date <= last_day(m.month_start)
    group by m.product_id, m.warehouse_id, m.month_start
),
with_demand as (
    select m.*, d.demand_units_trailing_90d, d.sales_cogs_trailing_90d
    from monthly_inventory m
    join trailing_demand d using (product_id, warehouse_id, month_start)
)
select
    md5(concat_ws('|', product_id, warehouse_id, month_start)) as inventory_monthly_key,
    product_id,
    warehouse_id,
    month_end_warehouse_key as warehouse_key,
    month_start,
    month_end_product_key as product_key,
    month_end_inventory_snapshot_key as inventory_snapshot_key,
    month_end_sku as sku,
    month_end_product_name as product_name,
    month_end_category as category,
    snapshot_count,
    avg_available_units,
    avg_inventory_value,
    month_end_available_units,
    month_end_on_hand_units,
    month_end_inventory_value,
    month_end_reorder_point as reorder_point,
    stockout_snapshot_count,
    snapshot_count - stockout_snapshot_count as in_stock_snapshot_count,
    stockout_snapshot_count / nullif(snapshot_count, 0)::float as stockout_rate,
    excess_unit_snapshot_sum / nullif(snapshot_count, 0) as avg_excess_units,
    greatest(month_end_available_units - coalesce(month_end_reorder_point, 0), 0) as month_end_excess_units,
    greatest(month_end_available_units - coalesce(month_end_reorder_point, 0), 0)
        * month_end_inventory_value / nullif(month_end_on_hand_units, 0) as month_end_excess_value,
    demand_units_trailing_90d,
    sales_cogs_trailing_90d,
    month_end_available_units > 0 and demand_units_trailing_90d = 0 as is_dead_stock_trailing_90d,
    iff(month_end_available_units > 0 and demand_units_trailing_90d = 0,
        month_end_available_units, 0) as dead_stock_units,
    iff(month_end_available_units > 0 and demand_units_trailing_90d = 0,
        month_end_available_units * month_end_inventory_value / nullif(month_end_on_hand_units, 0), 0) as dead_stock_value,
    sales_cogs_trailing_90d / nullif(avg_inventory_value, 0) as inventory_turnover_trailing_90d,
    avg_available_units / nullif(demand_units_trailing_90d / 90.0, 0) as days_of_supply_trailing_90d,
    month_end_quality_status as quality_status,
    month_end_product_history_fallback as product_history_fallback,
    month_end_batch_id as batch_id,
    warning_snapshot_count,
    product_history_fallback_snapshot_count,
    stockout_snapshot_count as stockout_numerator,
    snapshot_count as stockout_denominator,
    sales_cogs_trailing_90d as inventory_turnover_numerator,
    avg_inventory_value as inventory_turnover_denominator,
    demand_units_trailing_90d as days_of_supply_demand_numerator,
    cast(90 as number) as days_of_supply_period_denominator
from with_demand
