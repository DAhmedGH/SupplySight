{{ config(materialized='table') }}

with monthly as (
    select
        forecast_run_id,
        planning_cutoff,
        product_id,
        warehouse_id,
        warehouse_key,
        date_trunc('month', forecast_date)::date as forecast_month,
        max(available_units) as cutoff_available_units,
        iff(count_if(forecast_month_present) = count(*), sum(daily_point_demand), null) as point_demand_units,
        iff(count_if(daily_forecast_bounds_valid) = count(*), sum(daily_upper_demand), null) as upper_demand_units,
        sum(scheduled_inbound_units) as scheduled_inbound_units,
        max(iff(day(forecast_date) = 1, projected_point_inventory, null)) as first_day_point_inventory,
        max(iff(day(forecast_date) = day(last_day(forecast_date)), projected_point_inventory, null)) as closing_point_inventory,
        max(iff(day(forecast_date) = 1, projected_stress_inventory, null)) as first_day_stress_inventory,
        max(iff(day(forecast_date) = day(last_day(forecast_date)), projected_stress_inventory, null)) as closing_stress_inventory,
        count(*) as calendar_day_count,
        count_if(forecast_month_present) as forecast_day_count,
        count_if(daily_forecast_bounds_valid) as valid_bound_day_count,
        max(risk_assessable) as risk_assessable,
        max(completeness_reason) as completeness_reason,
        max(inventory_snapshot_key) as inventory_snapshot_key,
        max(snapshot_date) as snapshot_date,
        max(snapshot_date_key) as snapshot_date_key,
        max(selected_po_line_id) as selected_po_line_id,
        max(selected_supplier_key) as selected_supplier_key,
        max(selected_supplier_id) as selected_supplier_id,
        max(planned_lead_time_days) as planned_lead_time_days,
        max(supplier_selection_reason) as supplier_selection_reason,
        max(forecast_product_key) as forecast_product_key,
        max(snapshot_product_key) as snapshot_product_key,
        max(snapshot_product_history_fallback) as snapshot_product_history_fallback,
        max(has_forecast_fallback) as has_forecast_fallback,
        max(snapshot_quality_status) as snapshot_quality_status,
        max(uncertainty_method) as uncertainty_method,
        max(forecast_source_version) as forecast_source_version,
        max(forecast_model_version) as forecast_model_version
    from {{ ref('int_inventory_planning_daily') }}
    group by forecast_run_id, planning_cutoff, product_id, warehouse_id,
             warehouse_key, date_trunc('month', forecast_date)
), balances as (
    select m.*,
           case when row_number() over (
               partition by forecast_run_id, product_id, warehouse_key order by forecast_month
           ) = 1 then cutoff_available_units else lag(closing_point_inventory) over (
               partition by forecast_run_id, product_id, warehouse_key order by forecast_month
           ) end as opening_point_inventory,
           case when row_number() over (
               partition by forecast_run_id, product_id, warehouse_key order by forecast_month
           ) = 1 then cutoff_available_units else lag(closing_stress_inventory) over (
               partition by forecast_run_id, product_id, warehouse_key order by forecast_month
           ) end as opening_stress_inventory
    from monthly m
)
select
    forecast_run_id,
    planning_cutoff,
    product_id,
    warehouse_id,
    warehouse_key,
    forecast_month,
    inventory_snapshot_key,
    snapshot_date,
    snapshot_date_key,
    selected_po_line_id,
    selected_supplier_key,
    selected_supplier_id,
    planned_lead_time_days,
    supplier_selection_reason,
    forecast_product_key,
    snapshot_product_key,
    snapshot_product_history_fallback,
    snapshot_quality_status,
    uncertainty_method,
    forecast_source_version,
    forecast_model_version,
    cutoff_available_units,
    point_demand_units,
    upper_demand_units,
    scheduled_inbound_units,
    opening_point_inventory,
    closing_point_inventory,
    opening_stress_inventory,
    closing_stress_inventory,
    iff(valid_bound_day_count = calendar_day_count,
        greatest(upper_demand_units - point_demand_units, 0), null) as empirical_stress_buffer_units,
    forecast_day_count,
    valid_bound_day_count,
    calendar_day_count,
    forecast_day_count = calendar_day_count as forecast_month_complete,
    valid_bound_day_count = calendar_day_count as forecast_bounds_complete,
    risk_assessable,
    completeness_reason,
    has_forecast_fallback
from balances
