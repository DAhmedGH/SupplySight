{{ config(materialized='view') }}

with daily_forecast as (
    select
        f.run_id as forecast_run_id,
        f.product_id,
        f.warehouse_key,
        d.date_day as forecast_date,
        f.forecast_date as forecast_month,
        f.predicted_demand / day(last_day(f.forecast_date)) as daily_point_demand,
        f.upper_bound / day(last_day(f.forecast_date)) as daily_upper_demand,
        f.upper_bound,
        f.predicted_demand,
        f.fallback_used,
        f.product_key as forecast_product_key,
        f.model_name,
        f.uncertainty_method,
        f.source_version,
        f.model_version
    from {{ source('forecasting', 'current_forecasts') }} f
    join {{ ref('dim_date') }} d
      on d.date_day >= f.forecast_date
     and d.date_day < dateadd(month, 1, f.forecast_date)
    where d.date_day between to_date('2025-01-01') and to_date('2025-03-31')
), inbound_by_day as (
    select forecast_run_id, product_id, warehouse_key,
           expected_delivery_date as forecast_date,
           sum(scheduled_inbound_units) as scheduled_inbound_units
    from {{ ref('int_inventory_planning_supply') }}
    where inbound_classification = 'SCHEDULED_INBOUND'
      and expected_delivery_date between to_date('2025-01-01') and to_date('2025-03-31')
    group by forecast_run_id, product_id, warehouse_key, expected_delivery_date
), scaffold as (
    select p.forecast_run_id, p.planning_cutoff, p.product_id, p.warehouse_id,
           p.warehouse_key, p.inventory_snapshot_key, p.snapshot_date,
           p.snapshot_date_key, p.snapshot_product_key, p.forecast_product_key,
           p.on_hand_units, p.reserved_units, p.available_units,
           p.snapshot_in_transit_units_descriptive_only, p.snapshot_quality_status,
           p.snapshot_quality_issues, p.snapshot_product_history_fallback,
           p.snapshot_batch_id, p.snapshot_source_file, p.snapshot_source_system,
           p.snapshot_row_hash, p.selected_po_line_id, p.selected_supplier_key,
           p.selected_supplier_id, p.selected_po_order_date,
           p.selected_po_expected_delivery_date, p.selected_po_quality_status,
           p.selected_po_quality_issues, p.selected_po_product_history_fallback,
           p.selected_po_batch_id, p.selected_po_source_file,
           p.selected_po_source_system, p.selected_po_row_hash,
           p.planned_lead_time_days,
           p.supplier_selection_reason, p.scheduled_inbound_units_total,
           p.overdue_uncertain_units, p.open_uncertain_units,
           p.scheduled_po_line_count, p.scheduled_po_line_ids,
           p.historical_realized_unit_value, p.historical_delivered_units,
           p.has_sales_quality_warning, p.sales_product_history_fallback,
           p.forecast_run_status, p.observed_through, p.forecast_horizon,
           p.forecast_row_count, p.expected_month_row_count,
           p.valid_upper_bound_count, p.forecast_horizon_compatible,
           p.forecast_bounds_complete, p.cutoff_snapshot_available,
           p.risk_assessable, p.completeness_reason,
           p.has_forecast_fallback, p.selected_forecast_model,
           p.uncertainty_method, p.forecast_source_version,
           p.forecast_model_version,
           d.date_day as forecast_date
    from {{ ref('int_inventory_planning_position') }} p
    join {{ ref('dim_date') }} d
      on d.date_day between to_date('2025-01-01') and to_date('2025-03-31')
)
select
    s.forecast_run_id,
    s.planning_cutoff,
    s.product_id,
    s.warehouse_id,
    s.warehouse_key,
    s.forecast_date,
    date_trunc('month', s.forecast_date)::date as forecast_month,
    s.inventory_snapshot_key,
    s.snapshot_date,
    s.snapshot_date_key,
    s.snapshot_product_key,
    coalesce(df.forecast_product_key, s.forecast_product_key) as forecast_product_key,
    s.on_hand_units,
    s.reserved_units,
    s.available_units,
    s.snapshot_in_transit_units_descriptive_only,
    s.snapshot_batch_id,
    s.snapshot_source_file,
    s.snapshot_source_system,
    s.snapshot_row_hash,
    coalesce(inb.scheduled_inbound_units, 0) as scheduled_inbound_units,
    df.daily_point_demand,
    df.daily_upper_demand,
    df.predicted_demand as monthly_point_forecast,
    df.upper_bound as monthly_upper_forecast,
    df.forecast_month is not null as forecast_month_present,
    iff(df.forecast_month is not null and df.upper_bound is not null
        and df.upper_bound >= df.predicted_demand, true, false) as daily_forecast_bounds_valid,
    s.completeness_reason,
    s.risk_assessable,
    s.forecast_horizon_compatible,
    s.forecast_bounds_complete,
    s.selected_po_line_id,
    s.selected_supplier_key,
    s.selected_supplier_id,
    s.selected_po_order_date,
    s.selected_po_expected_delivery_date,
    s.selected_po_quality_status,
    s.selected_po_quality_issues,
    s.selected_po_product_history_fallback,
    s.selected_po_batch_id,
    s.selected_po_source_file,
    s.selected_po_source_system,
    s.selected_po_row_hash,
    s.planned_lead_time_days,
    s.supplier_selection_reason,
    s.historical_realized_unit_value,
    s.historical_delivered_units,
    s.has_sales_quality_warning,
    s.sales_product_history_fallback,
    s.snapshot_quality_status,
    s.snapshot_quality_issues,
    s.snapshot_product_history_fallback,
    s.scheduled_inbound_units_total,
    s.overdue_uncertain_units,
    s.open_uncertain_units,
    s.scheduled_po_line_count,
    s.scheduled_po_line_ids,
    s.forecast_run_status,
    s.observed_through,
    s.forecast_horizon,
    s.forecast_row_count,
    s.expected_month_row_count,
    s.valid_upper_bound_count,
    s.cutoff_snapshot_available,
    s.selected_forecast_model,
    s.has_forecast_fallback,
    df.fallback_used as daily_forecast_fallback_used,
    df.model_name as daily_forecast_model,
    df.uncertainty_method,
    s.forecast_source_version,
    s.forecast_model_version,
    sum(coalesce(inb.scheduled_inbound_units, 0)) over (
        partition by s.forecast_run_id, s.product_id, s.warehouse_key
        order by s.forecast_date rows between unbounded preceding and current row
    ) as cumulative_scheduled_inbound_units,
    sum(coalesce(df.daily_point_demand, 0)) over (
        partition by s.forecast_run_id, s.product_id, s.warehouse_key
        order by s.forecast_date rows between unbounded preceding and current row
    ) as cumulative_point_demand,
    sum(coalesce(df.daily_upper_demand, 0)) over (
        partition by s.forecast_run_id, s.product_id, s.warehouse_key
        order by s.forecast_date rows between unbounded preceding and current row
    ) as cumulative_upper_demand,
    case when s.risk_assessable and df.forecast_month is not null
          and s.available_units is not null
          and s.forecast_bounds_complete
         then s.available_units +
              sum(coalesce(inb.scheduled_inbound_units, 0)) over (
                partition by s.forecast_run_id, s.product_id, s.warehouse_key
                order by s.forecast_date rows between unbounded preceding and current row
              ) -
              sum(coalesce(df.daily_point_demand, 0)) over (
                partition by s.forecast_run_id, s.product_id, s.warehouse_key
                order by s.forecast_date rows between unbounded preceding and current row
              ) end as projected_point_inventory,
    case when s.risk_assessable and df.forecast_month is not null
          and s.available_units is not null
          and s.forecast_bounds_complete
         then s.available_units +
              sum(coalesce(inb.scheduled_inbound_units, 0)) over (
                partition by s.forecast_run_id, s.product_id, s.warehouse_key
                order by s.forecast_date rows between unbounded preceding and current row
              ) -
              sum(coalesce(df.daily_upper_demand, 0)) over (
                partition by s.forecast_run_id, s.product_id, s.warehouse_key
                order by s.forecast_date rows between unbounded preceding and current row
              ) end as projected_stress_inventory,
    case when s.risk_assessable and df.forecast_month is not null
          and s.available_units is not null and s.forecast_bounds_complete
         then greatest(-(
              s.available_units +
              sum(coalesce(inb.scheduled_inbound_units, 0)) over (
                partition by s.forecast_run_id, s.product_id, s.warehouse_key
                order by s.forecast_date rows between unbounded preceding and current row
              ) -
              sum(coalesce(df.daily_point_demand, 0)) over (
                partition by s.forecast_run_id, s.product_id, s.warehouse_key
                order by s.forecast_date rows between unbounded preceding and current row
              )), 0) end as point_deficit_units,
    case when s.risk_assessable and df.forecast_month is not null
          and s.available_units is not null and s.forecast_bounds_complete
         then greatest(-(
              s.available_units +
              sum(coalesce(inb.scheduled_inbound_units, 0)) over (
                partition by s.forecast_run_id, s.product_id, s.warehouse_key
                order by s.forecast_date rows between unbounded preceding and current row
              ) -
              sum(coalesce(df.daily_upper_demand, 0)) over (
                partition by s.forecast_run_id, s.product_id, s.warehouse_key
                order by s.forecast_date rows between unbounded preceding and current row
              )), 0) end as stress_deficit_units
from scaffold s
left join daily_forecast df on s.forecast_run_id = df.forecast_run_id
  and s.product_id = df.product_id and s.warehouse_key = df.warehouse_key
  and s.forecast_date = df.forecast_date
left join inbound_by_day inb on s.forecast_run_id = inb.forecast_run_id
  and s.product_id = inb.product_id and s.warehouse_key = inb.warehouse_key
  and s.forecast_date = inb.forecast_date
