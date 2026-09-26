{{ config(materialized='table') }}

with series as (
    select
        forecast_run_id,
        planning_cutoff,
        product_id,
        warehouse_id,
        warehouse_key,
        max(inventory_snapshot_key) as inventory_snapshot_key,
        max(snapshot_date) as snapshot_date,
        max(snapshot_date_key) as snapshot_date_key,
        max(snapshot_product_key) as snapshot_product_key,
        max(forecast_product_key) as forecast_product_key,
        max(on_hand_units) as on_hand_units,
        max(reserved_units) as reserved_units,
        max(available_units) as available_units,
        max(snapshot_in_transit_units_descriptive_only) as snapshot_in_transit_units_descriptive_only,
        max(snapshot_quality_status) as snapshot_quality_status,
        any_value(snapshot_quality_issues) as snapshot_quality_issues,
        max(snapshot_product_history_fallback) as snapshot_product_history_fallback,
        max(snapshot_batch_id) as snapshot_batch_id,
        max(snapshot_source_file) as snapshot_source_file,
        max(snapshot_source_system) as snapshot_source_system,
        max(snapshot_row_hash) as snapshot_row_hash,
        max(selected_po_line_id) as selected_po_line_id,
        max(selected_supplier_key) as selected_supplier_key,
        max(selected_supplier_id) as selected_supplier_id,
        max(selected_po_quality_status) as selected_po_quality_status,
        any_value(selected_po_quality_issues) as selected_po_quality_issues,
        max(selected_po_product_history_fallback) as selected_po_product_history_fallback,
        max(selected_po_batch_id) as selected_po_batch_id,
        max(selected_po_source_file) as selected_po_source_file,
        max(selected_po_source_system) as selected_po_source_system,
        max(selected_po_row_hash) as selected_po_row_hash,
        max(selected_po_order_date) as selected_po_order_date,
        max(selected_po_expected_delivery_date) as selected_po_expected_delivery_date,
        max(planned_lead_time_days) as planned_lead_time_days,
        max(supplier_selection_reason) as supplier_selection_reason,
        max(scheduled_inbound_units_total) as scheduled_inbound_units_total,
        max(overdue_uncertain_units) as overdue_uncertain_units,
        max(open_uncertain_units) as open_uncertain_units,
        max(scheduled_po_line_count) as scheduled_po_line_count,
        max(scheduled_po_line_ids) as scheduled_po_line_ids,
        max(historical_realized_unit_value) as historical_realized_unit_value,
        max(historical_delivered_units) as historical_delivered_units,
        max(has_sales_quality_warning) as has_sales_quality_warning,
        max(sales_product_history_fallback) as sales_product_history_fallback,
        max(forecast_run_status) as forecast_run_status,
        max(observed_through) as observed_through,
        max(forecast_horizon) as forecast_horizon,
        max(forecast_row_count) as forecast_row_count,
        max(expected_month_row_count) as expected_month_row_count,
        max(valid_upper_bound_count) as valid_upper_bound_count,
        max(forecast_horizon_compatible) as forecast_horizon_compatible,
        max(forecast_bounds_complete) as forecast_bounds_complete,
        max(cutoff_snapshot_available) as cutoff_snapshot_available,
        max(risk_assessable) as risk_assessable,
        max(completeness_reason) as completeness_reason,
        max(has_forecast_fallback) as has_forecast_fallback,
        max(selected_forecast_model) as selected_forecast_model,
        max(uncertainty_method) as uncertainty_method,
        max(forecast_source_version) as forecast_source_version,
        max(forecast_model_version) as forecast_model_version,
        max(point_deficit_units) as expected_shortage_units,
        max(stress_deficit_units) as stressed_shortage_units,
        min(iff(projected_point_inventory < 0, forecast_date, null)) as first_expected_shortage_date,
        min(iff(projected_stress_inventory < 0, forecast_date, null)) as first_stressed_shortage_date,
        sum(iff(forecast_date between dateadd(day, 1, planning_cutoff)
                    and dateadd(day, planned_lead_time_days, planning_cutoff),
                daily_point_demand, 0)) as point_lead_time_demand,
        sum(iff(forecast_date between dateadd(day, 1, planning_cutoff)
                    and dateadd(day, planned_lead_time_days, planning_cutoff),
                daily_upper_demand, 0)) as upper_lead_time_demand,
        count_if(forecast_date between dateadd(day, 1, planning_cutoff)
                    and dateadd(day, planned_lead_time_days, planning_cutoff)
                 and daily_point_demand is not null) as point_lead_days_present,
        count_if(forecast_date between dateadd(day, 1, planning_cutoff)
                    and dateadd(day, planned_lead_time_days, planning_cutoff)
                 and daily_upper_demand is not null) as upper_lead_days_present,
        max(iff(forecast_date >= dateadd(day, planned_lead_time_days, planning_cutoff),
                stress_deficit_units, null)) as max_post_arrival_stress_deficit,
        min(iff(forecast_date < dateadd(day, planned_lead_time_days, planning_cutoff)
                 and (projected_point_inventory < 0 or projected_stress_inventory < 0),
                forecast_date, null)) as shortage_before_reorder_arrival
    from {{ ref('int_inventory_planning_daily') }}
    group by forecast_run_id, planning_cutoff, product_id, warehouse_id, warehouse_key
), calculated as (
    select s.*,
           dateadd(day, planned_lead_time_days, planning_cutoff) as standard_reorder_arrival_date,
           datediff(day, dateadd(day, 1, planning_cutoff), to_date('2025-03-31')) + 1 as horizon_days,
           (cutoff_snapshot_available and forecast_horizon_compatible
             and forecast_bounds_complete and forecast_row_count = 3) as planning_forecast_complete,
           (planned_lead_time_days > 0 and selected_po_line_id is not null) as lead_time_available,
           (planned_lead_time_days > 0 and selected_po_line_id is not null
             and dateadd(day, planned_lead_time_days, planning_cutoff) <= to_date('2025-03-31')) as reorder_arrival_in_horizon
    from series s
), risk as (
    select c.*,
           case
             when not risk_assessable then 'UNASSESSABLE'
             when available_units = 0 then 'CRITICAL'
             when expected_shortage_units > 0 then 'HIGH'
             when stressed_shortage_units > 0 then 'MEDIUM'
             else 'LOW'
           end as stockout_risk_tier,
           case
             when not cutoff_snapshot_available then 'MISSING_CUTOFF_SNAPSHOT'
             when forecast_row_count = 0 then 'MISSING_FORECAST_SERIES'
             when not forecast_horizon_compatible then
               iff(observed_through is distinct from planning_cutoff,
                   'INCOMPATIBLE_FORECAST_CUTOFF', 'INCOMPLETE_FORECAST_HORIZON')
             when not forecast_bounds_complete then 'MISSING_FORECAST_BOUNDS'
             when selected_po_line_id is null then 'NO_SUPPLIER_RELATIONSHIP'
             when planned_lead_time_days is null or planned_lead_time_days <= 0 then 'INVALID_LEAD_TIME'
             when not reorder_arrival_in_horizon then 'LEAD_TIME_EXCEEDS_FORECAST_HORIZON'
             else 'AVAILABLE'
           end as reorder_unavailable_reason
    from calculated c
)
select
    forecast_run_id,
    planning_cutoff,
    product_id,
    warehouse_id,
    warehouse_key,
    inventory_snapshot_key,
    snapshot_date,
    snapshot_date_key,
    snapshot_product_key,
    forecast_product_key,
    on_hand_units,
    reserved_units,
    available_units,
    snapshot_in_transit_units_descriptive_only,
    scheduled_inbound_units_total,
    overdue_uncertain_units,
    open_uncertain_units,
    scheduled_po_line_count,
    scheduled_po_line_ids,
    selected_po_line_id,
    selected_supplier_key,
    selected_supplier_id,
    selected_po_quality_status,
    selected_po_quality_issues,
    selected_po_product_history_fallback,
    selected_po_batch_id,
    selected_po_source_file,
    selected_po_source_system,
    selected_po_row_hash,
    selected_po_order_date,
    selected_po_expected_delivery_date,
    planned_lead_time_days,
    standard_reorder_arrival_date,
    supplier_selection_reason,
    historical_realized_unit_value,
    historical_delivered_units,
    has_sales_quality_warning,
    sales_product_history_fallback,
    expected_shortage_units,
    stressed_shortage_units,
    first_expected_shortage_date,
    first_stressed_shortage_date,
    stockout_risk_tier,
    iff(reorder_unavailable_reason = 'AVAILABLE',
        ceil(greatest(coalesce(max_post_arrival_stress_deficit, 0), 0)), null) as suggested_reorder_units,
    (reorder_unavailable_reason = 'AVAILABLE') as reorder_recommendation_available,
    iff(reorder_unavailable_reason = 'AVAILABLE', null, reorder_unavailable_reason) as reorder_unavailable_reason,
    iff(reorder_unavailable_reason = 'AVAILABLE',
        coalesce(shortage_before_reorder_arrival is not null, false), null) as immediate_mitigation_flag,
    point_lead_time_demand,
    upper_lead_time_demand,
    iff(lead_time_available and reorder_arrival_in_horizon
        and point_lead_days_present = planned_lead_time_days
        and upper_lead_days_present = planned_lead_time_days,
        greatest(upper_lead_time_demand - point_lead_time_demand, 0), null) as empirical_stress_buffer_units,
    iff(historical_realized_unit_value is not null,
        expected_shortage_units * historical_realized_unit_value, null) as potential_revenue_exposure,
    completeness_reason,
    forecast_run_status,
    observed_through,
    forecast_horizon,
    forecast_horizon_compatible,
    forecast_bounds_complete,
    forecast_row_count,
    expected_month_row_count,
    valid_upper_bound_count,
    selected_forecast_model,
    uncertainty_method,
    forecast_source_version,
    forecast_model_version,
    has_forecast_fallback,
    snapshot_quality_status,
    snapshot_quality_issues,
    snapshot_product_history_fallback,
    snapshot_batch_id,
    snapshot_source_file,
    snapshot_source_system,
    snapshot_row_hash,
    cutoff_snapshot_available,
    risk_assessable
from risk
