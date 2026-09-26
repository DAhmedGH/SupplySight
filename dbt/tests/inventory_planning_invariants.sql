{{ config(tags=['inventory_intelligence']) }}

with bad_snapshot as (
    select 'cutoff_snapshot_not_exact_or_missing_as_zero' as failure,
           concat_ws('|', forecast_run_id, product_id, warehouse_key) as detail
    from {{ ref('int_inventory_planning_position') }}
    where (cutoff_snapshot_available and snapshot_date <> to_date('2024-12-31'))
       or (not cutoff_snapshot_available and
           (inventory_snapshot_key is not null or snapshot_date is not null or available_units is not null))
), bad_inbound as (
    select 'scheduled_po_rule' as failure, concat_ws('|', forecast_run_id, po_line_id) as detail
    from {{ ref('int_inventory_planning_supply') }}
    where inbound_classification = 'SCHEDULED_INBOUND'
      and (order_date > planning_cutoff or po_status <> 'in_transit'
           or received_date is not null or received_quantity <> 0
           or remaining_units <= 0 or expected_delivery_date <= planning_cutoff
           or scheduled_inbound_units <> remaining_units)
    union all
    select 'uncertain_inbound_in_scheduled_total', concat_ws('|', forecast_run_id, po_line_id)
    from {{ ref('int_inventory_planning_supply') }}
    where inbound_classification in ('OVERDUE_UNCERTAIN', 'OPEN_UNCERTAIN')
      and scheduled_inbound_units <> 0
    union all
    select 'future_order_in_planning_state', concat_ws('|', forecast_run_id, po_line_id)
    from {{ ref('int_inventory_planning_supply') }}
    where inbound_classification = 'SCHEDULED_INBOUND' and order_date > planning_cutoff
), selected_supplier as (
    with latest as (
        select forecast_run_id, product_id, warehouse_key, po_line_id,
               row_number() over (partition by forecast_run_id, product_id, warehouse_key
                 order by order_date desc, po_line_id desc) as rn
        from {{ ref('int_inventory_planning_supply') }}
        where order_date <= to_date('2024-12-31')
          and inbound_classification not in ('FUTURE_ORDER_EXCLUDED', 'POST_CUTOFF_RECEIPT_DATE_EXCLUDED')
    )
    select p.forecast_run_id, p.product_id, p.warehouse_key,
           p.selected_po_line_id, l.po_line_id as expected_po_line_id
    from {{ ref('int_inventory_planning_position') }} p
    left join latest l on p.forecast_run_id = l.forecast_run_id
      and p.product_id = l.product_id and p.warehouse_key = l.warehouse_key and l.rn = 1
), bad_supplier as (
    select 'supplier_selection_not_latest' as failure,
           concat_ws('|', forecast_run_id, product_id, warehouse_key) as detail
    from selected_supplier
    where selected_po_line_id is distinct from expected_po_line_id
), inbound_reconciliation as (
    select p.forecast_run_id, p.product_id, p.warehouse_key,
           p.scheduled_inbound_units_total,
           coalesce(sum(s.scheduled_inbound_units), 0) as expected_scheduled_units
    from {{ ref('int_inventory_planning_position') }} p
    left join {{ ref('int_inventory_planning_supply') }} s
      on p.forecast_run_id = s.forecast_run_id
     and p.product_id = s.product_id and p.warehouse_key = s.warehouse_key
    group by p.forecast_run_id, p.product_id, p.warehouse_key, p.scheduled_inbound_units_total
), bad_inbound_reconciliation as (
    select 'scheduled_inbound_counted_once' as failure,
           concat_ws('|', forecast_run_id, product_id, warehouse_key) as detail
    from inbound_reconciliation
    where scheduled_inbound_units_total <> expected_scheduled_units
), daily_inbound_reconciliation as (
    select d.forecast_run_id, d.product_id, d.warehouse_key,
           sum(d.scheduled_inbound_units) as daily_scheduled_units,
           coalesce(max(s.scheduled_horizon_units), 0) as po_scheduled_horizon_units
    from {{ ref('int_inventory_planning_daily') }} d
    left join (
        select forecast_run_id, product_id, warehouse_key,
               sum(scheduled_inbound_units) as scheduled_horizon_units
        from {{ ref('int_inventory_planning_supply') }}
        where inbound_classification = 'SCHEDULED_INBOUND'
          and expected_delivery_date between to_date('2025-01-01') and to_date('2025-03-31')
        group by forecast_run_id, product_id, warehouse_key
    ) s on d.forecast_run_id = s.forecast_run_id
      and d.product_id = s.product_id and d.warehouse_key = s.warehouse_key
    group by d.forecast_run_id, d.product_id, d.warehouse_key
), bad_daily_inbound as (
    select 'daily_scheduled_inbound_reconciliation' as failure,
           concat_ws('|', forecast_run_id, product_id, warehouse_key) as detail
    from daily_inbound_reconciliation
    where daily_scheduled_units <> po_scheduled_horizon_units
), bad_proration as (
    select 'daily_monthly_forecast_reconciliation' as failure,
           concat_ws('|', d.forecast_run_id, d.product_id, d.warehouse_key, d.forecast_month) as detail
    from (
        select forecast_run_id, product_id, warehouse_key,
               date_trunc('month', forecast_date)::date as forecast_month,
               sum(daily_point_demand) as point_sum,
               sum(daily_upper_demand) as upper_sum,
               max(monthly_point_forecast) as monthly_point,
               max(monthly_upper_forecast) as monthly_upper
        from {{ ref('int_inventory_planning_daily') }}
        where forecast_month_present and daily_forecast_bounds_valid
        group by forecast_run_id, product_id, warehouse_key, date_trunc('month', forecast_date)
    ) d
    where abs(d.point_sum - d.monthly_point) > 0.000001
       or abs(d.upper_sum - d.monthly_upper) > 0.000001
), bad_population as (
    select 'missing_recommendation_population' as failure,
           concat_ws('|', p.forecast_run_id, p.product_id, p.warehouse_key) as detail
    from {{ ref('int_inventory_planning_position') }} p
    left join {{ ref('mart_inventory_recommendations') }} r
      on p.forecast_run_id = r.forecast_run_id and p.planning_cutoff = r.planning_cutoff
     and p.product_id = r.product_id and p.warehouse_key = r.warehouse_key
    where r.forecast_run_id is null
    union all
    select 'unexpected_recommendation_population',
           concat_ws('|', r.forecast_run_id, r.product_id, r.warehouse_key)
    from {{ ref('mart_inventory_recommendations') }} r
    left join {{ ref('int_inventory_planning_position') }} p
      on p.forecast_run_id = r.forecast_run_id and p.planning_cutoff = r.planning_cutoff
     and p.product_id = r.product_id and p.warehouse_key = r.warehouse_key
    where p.forecast_run_id is null
), missing_cutoff_inventory_population as (
    select 'missing_cutoff_inventory_position' as failure,
           concat_ws('|', r.forecast_run_id, i.product_id, i.warehouse_key) as detail
    from (select distinct run_id as forecast_run_id
          from {{ source('forecasting', 'current_forecasts') }}) r
    cross join {{ ref('inventory_detail') }} i
    left join {{ ref('int_inventory_planning_position') }} p
      on r.forecast_run_id = p.forecast_run_id
     and i.product_id = p.product_id and i.warehouse_key = p.warehouse_key
    where i.snapshot_date = to_date('2024-12-31')
      and p.forecast_run_id is null
), delivered_by_cutoff as (
    select order_line_id, sum(shipped_quantity) as delivered_units
    from {{ ref('shipment_detail') }}
    where shipment_status = 'delivered'
      and delivery_date < to_date('2024-12-31')
    group by order_line_id
), historical_revenue_expected as (
    select product.product_id, sale.warehouse_key,
           sum(sale.quantity) as delivered_units,
           sum(sale.revenue_amount) as delivered_revenue
    from {{ ref('sales_order_line') }} sale
    join {{ ref('dim_date') }} date_dim on sale.order_date_key = date_dim.date_key
    join {{ ref('dim_product') }} product on sale.product_key = product.product_key
    join delivered_by_cutoff shipment on sale.order_line_id = shipment.order_line_id
      and shipment.delivered_units >= sale.quantity
    where date_dim.date_day < to_date('2024-12-31')
      and sale.order_status = 'delivered' and sale.is_revenue_recognized
      and sale.quality_status in ('CLEAN', 'WARNING')
    group by product.product_id, sale.warehouse_key
), bad_historical_revenue as (
    select 'historical_revenue_as_of_leakage' as failure,
           concat_ws('|', p.forecast_run_id, p.product_id, p.warehouse_key) as detail
    from {{ ref('int_inventory_planning_position') }} p
    left join historical_revenue_expected expected
      on p.product_id = expected.product_id and p.warehouse_key = expected.warehouse_key
    where p.historical_delivered_units is distinct from expected.delivered_units
       or p.historical_delivered_revenue is distinct from expected.delivered_revenue
), bad_metrics as (
    select 'negative_recommendation_metric' as failure,
           concat_ws('|', forecast_run_id, product_id, warehouse_key) as detail
    from {{ ref('mart_inventory_recommendations') }}
    where (expected_shortage_units is not null and expected_shortage_units < 0)
       or (stressed_shortage_units is not null and stressed_shortage_units < 0)
       or (suggested_reorder_units is not null and suggested_reorder_units < 0)
       or (potential_revenue_exposure is not null and potential_revenue_exposure < 0)
)
select * from bad_snapshot
union all select * from bad_inbound
union all select * from bad_supplier
union all select * from bad_inbound_reconciliation
union all select * from bad_daily_inbound
union all select * from bad_metrics
union all select * from bad_proration
union all select * from bad_population
union all select * from missing_cutoff_inventory_population
union all select * from bad_historical_revenue
