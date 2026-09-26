{{ config(materialized='view') }}

with forecast_values as (
    select
        f.run_id as forecast_run_id,
        to_date('2024-12-31') as planning_cutoff,
        f.product_id,
        f.warehouse_key,
        count(*) as forecast_row_count,
        count(distinct f.forecast_date) as distinct_forecast_month_count,
        count_if(f.forecast_date in (to_date('2025-01-01'), to_date('2025-02-01'), to_date('2025-03-01'))) as expected_month_row_count,
        count_if(f.upper_bound is not null and f.upper_bound >= f.predicted_demand) as valid_upper_bound_count,
        min(f.forecast_date) as first_forecast_month,
        max(f.forecast_date) as last_forecast_month,
        max(f.product_key) as forecast_product_key,
        max(iff(f.fallback_used, 1, 0)) = 1 as has_forecast_fallback,
        max(f.model_name) as selected_forecast_model,
        max(f.uncertainty_method) as uncertainty_method,
        max(f.source_version) as forecast_source_version,
        max(f.model_version) as forecast_model_version
    from {{ source('forecasting', 'current_forecasts') }} f
    group by f.run_id, f.product_id, f.warehouse_key
), compatible_runs as (
    select run_id as forecast_run_id, status as forecast_run_status,
           observed_through, horizon, series_succeeded, records_produced
    from {{ source('forecasting', 'run_metadata') }}
    where upper(status) = 'SUCCEEDED'
), snapshots as (
    select i.*
    from {{ ref('inventory_detail') }} i
    where i.snapshot_date = to_date('2024-12-31')
), planning_pairs as (
    select forecast_run_id, product_id, warehouse_key from forecast_values
    union
    select r.forecast_run_id, s.product_id, s.warehouse_key
    from (select distinct run_id as forecast_run_id
          from {{ source('forecasting', 'current_forecasts') }}) r
    cross join snapshots s
), forecast_series as (
    select p.forecast_run_id, to_date('2024-12-31') as planning_cutoff,
           p.product_id, p.warehouse_key,
           coalesce(v.forecast_row_count, 0) as forecast_row_count,
           coalesce(v.distinct_forecast_month_count, 0) as distinct_forecast_month_count,
           coalesce(v.expected_month_row_count, 0) as expected_month_row_count,
           coalesce(v.valid_upper_bound_count, 0) as valid_upper_bound_count,
           v.first_forecast_month, v.last_forecast_month, v.forecast_product_key,
           coalesce(v.has_forecast_fallback, false) as has_forecast_fallback,
           v.selected_forecast_model, v.uncertainty_method,
           v.forecast_source_version, v.forecast_model_version
    from planning_pairs p
    left join forecast_values v on p.forecast_run_id = v.forecast_run_id
      and p.product_id = v.product_id and p.warehouse_key = v.warehouse_key
), latest_po as (
    select *
    from {{ ref('int_inventory_planning_supply') }}
    where order_date <= to_date('2024-12-31')
      and inbound_classification not in ('FUTURE_ORDER_EXCLUDED', 'POST_CUTOFF_RECEIPT_DATE_EXCLUDED')
    qualify row_number() over (
        partition by forecast_run_id, product_id, warehouse_key
        order by order_date desc, po_line_id desc
    ) = 1
), inbound_context as (
    select forecast_run_id, product_id, warehouse_key,
           sum(scheduled_inbound_units) as scheduled_inbound_units_total,
           sum(iff(inbound_classification = 'OVERDUE_UNCERTAIN', remaining_units, 0)) as overdue_uncertain_units,
           sum(iff(inbound_classification = 'OPEN_UNCERTAIN', remaining_units, 0)) as open_uncertain_units,
           count_if(inbound_classification = 'SCHEDULED_INBOUND') as scheduled_po_line_count,
           listagg(iff(inbound_classification = 'SCHEDULED_INBOUND', po_line_id, null), ',')
             within group (order by expected_delivery_date, po_line_id) as scheduled_po_line_ids
    from {{ ref('int_inventory_planning_supply') }}
    group by forecast_run_id, product_id, warehouse_key
), delivered_by_cutoff as (
    select order_line_id, sum(shipped_quantity) as delivered_units_by_cutoff
    from {{ ref('shipment_detail') }}
    where shipment_status = 'delivered'
      and delivery_date < to_date('2024-12-31')
    group by order_line_id
), historical_value as (
    select p.product_id, s.warehouse_key,
           sum(s.revenue_amount) as historical_delivered_revenue,
           sum(s.quantity) as historical_delivered_units,
           sum(s.revenue_amount) / nullif(sum(s.quantity), 0) as historical_realized_unit_value,
           count(*) as delivered_order_line_count,
           max(iff(s.quality_status = 'WARNING', 1, 0)) = 1 as has_sales_quality_warning,
           max(iff(s.product_history_fallback, 1, 0)) = 1 as sales_product_history_fallback
    from {{ ref('sales_order_line') }} s
    join delivered_by_cutoff shipped on s.order_line_id = shipped.order_line_id
      and shipped.delivered_units_by_cutoff >= s.quantity
    join {{ ref('dim_product') }} p on s.product_key = p.product_key
    join {{ ref('dim_date') }} d on s.order_date_key = d.date_key
    where d.date_day < to_date('2024-12-31')
      and s.order_status = 'delivered'
      and s.is_revenue_recognized
      and s.quality_status in ('CLEAN', 'WARNING')
    group by p.product_id, s.warehouse_key
)
select
    f.forecast_run_id,
    f.planning_cutoff,
    f.product_id,
    w.warehouse_id,
    f.warehouse_key,
    s.inventory_snapshot_key,
    s.snapshot_date,
    s.snapshot_date_key,
    s.product_key as snapshot_product_key,
    f.forecast_product_key,
    s.on_hand_units,
    s.reserved_units,
    s.available_units,
    s.in_transit_units as snapshot_in_transit_units_descriptive_only,
    s.quality_status as snapshot_quality_status,
    s.quality_issues as snapshot_quality_issues,
    s.product_history_fallback as snapshot_product_history_fallback,
    s.batch_id as snapshot_batch_id,
    s.source_file as snapshot_source_file,
    s.source_system as snapshot_source_system,
    s.row_hash as snapshot_row_hash,
    lp.po_line_id as selected_po_line_id,
    lp.purchase_order_id as selected_purchase_order_id,
    lp.supplier_key as selected_supplier_key,
    lp.supplier_id as selected_supplier_id,
    lp.order_date as selected_po_order_date,
    lp.expected_delivery_date as selected_po_expected_delivery_date,
    lp.quality_status as selected_po_quality_status,
    lp.quality_issues as selected_po_quality_issues,
    lp.product_history_fallback as selected_po_product_history_fallback,
    lp.batch_id as selected_po_batch_id,
    lp.source_file as selected_po_source_file,
    lp.source_system as selected_po_source_system,
    lp.row_hash as selected_po_row_hash,
    lp.planned_lead_time_days,
    iff(lp.po_line_id is null, 'NO_ELIGIBLE_PO_RELATIONSHIP', 'MOST_RECENT_ELIGIBLE_PO') as supplier_selection_reason,
    coalesce(ic.scheduled_inbound_units_total, 0) as scheduled_inbound_units_total,
    coalesce(ic.overdue_uncertain_units, 0) as overdue_uncertain_units,
    coalesce(ic.open_uncertain_units, 0) as open_uncertain_units,
    coalesce(ic.scheduled_po_line_count, 0) as scheduled_po_line_count,
    ic.scheduled_po_line_ids,
    hv.historical_delivered_revenue,
    hv.historical_delivered_units,
    hv.historical_realized_unit_value,
    coalesce(hv.has_sales_quality_warning, false) as has_sales_quality_warning,
    coalesce(hv.sales_product_history_fallback, false) as sales_product_history_fallback,
    coalesce(hv.delivered_order_line_count, 0) as delivered_order_line_count,
    cr.forecast_run_status,
    cr.observed_through,
    cr.horizon as forecast_horizon,
    cr.series_succeeded,
    cr.records_produced,
    f.forecast_row_count,
    f.distinct_forecast_month_count,
    f.expected_month_row_count,
    f.valid_upper_bound_count,
    f.first_forecast_month,
    f.last_forecast_month,
    f.has_forecast_fallback,
    f.selected_forecast_model,
    f.uncertainty_method,
    f.forecast_source_version,
    f.forecast_model_version,
    coalesce((cr.observed_through = to_date('2024-12-31') and cr.horizon = 3
      and f.forecast_row_count = 3 and f.distinct_forecast_month_count = 3
      and f.expected_month_row_count = 3
      and f.first_forecast_month = to_date('2025-01-01')
      and f.last_forecast_month = to_date('2025-03-01')), false) as forecast_horizon_compatible,
    (f.valid_upper_bound_count = 3) as forecast_bounds_complete,
    (s.inventory_snapshot_key is not null) as cutoff_snapshot_available,
    coalesce((s.inventory_snapshot_key is not null
      and cr.observed_through = to_date('2024-12-31') and cr.horizon = 3
      and f.forecast_row_count = 3 and f.distinct_forecast_month_count = 3
      and f.expected_month_row_count = 3
      and f.first_forecast_month = to_date('2025-01-01')
      and f.last_forecast_month = to_date('2025-03-01')
      and f.valid_upper_bound_count = 3), false) as risk_assessable,
    case
      when s.inventory_snapshot_key is null then 'MISSING_CUTOFF_SNAPSHOT'
      when cr.observed_through is distinct from to_date('2024-12-31') then 'INCOMPATIBLE_FORECAST_CUTOFF'
      when f.forecast_row_count = 0 then 'MISSING_FORECAST_SERIES'
      when cr.horizon is distinct from 3 or f.forecast_row_count <> 3
        or f.distinct_forecast_month_count <> 3
        or f.expected_month_row_count <> 3
        or f.first_forecast_month is distinct from to_date('2025-01-01')
        or f.last_forecast_month is distinct from to_date('2025-03-01') then 'INCOMPLETE_FORECAST_HORIZON'
      when f.valid_upper_bound_count <> 3 then 'MISSING_FORECAST_BOUNDS'
      else 'COMPLETE'
    end as completeness_reason
from forecast_series f
left join compatible_runs cr on f.forecast_run_id = cr.forecast_run_id
left join {{ ref('dim_warehouse') }} w on f.warehouse_key = w.warehouse_key
left join snapshots s on f.product_id = s.product_id and f.warehouse_key = s.warehouse_key
left join latest_po lp on f.forecast_run_id = lp.forecast_run_id
  and f.product_id = lp.product_id and f.warehouse_key = lp.warehouse_key
left join inbound_context ic on f.forecast_run_id = ic.forecast_run_id
  and f.product_id = ic.product_id and f.warehouse_key = ic.warehouse_key
left join historical_value hv on f.product_id = hv.product_id and f.warehouse_key = hv.warehouse_key
