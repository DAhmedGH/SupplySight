{{ config(materialized='table') }}

with protection as (
    select
        forecast_run_id,
        product_id,
        warehouse_id,
        warehouse_key,
        max(available_units) as source_available_units,
        max(inventory_snapshot_key) as source_inventory_snapshot_key,
        max(selected_po_line_id) as source_selected_po_line_id,
        max(risk_assessable) as source_risk_assessable,
        max(iff(risk_assessable,
            greatest(cumulative_upper_demand - cumulative_scheduled_inbound_units, 0), null))
            as source_protected_requirement_units
    from {{ ref('int_inventory_planning_daily') }}
    group by forecast_run_id, product_id, warehouse_id, warehouse_key
), source_surplus as (
    select *,
           floor(greatest(source_available_units - source_protected_requirement_units, 0)) as transferable_surplus_units
    from protection
    where source_risk_assessable
      and source_available_units is not null
      and source_protected_requirement_units is not null
), destination_need as (
    select forecast_run_id, product_id, warehouse_id, warehouse_key,
           expected_shortage_units,
           stockout_risk_tier,
           inventory_snapshot_key,
           selected_po_line_id,
           selected_supplier_key,
           floor(greatest(expected_shortage_units, 0)) as transfer_need_units
    from {{ ref('mart_inventory_recommendations') }}
    where risk_assessable and expected_shortage_units > 0
), ranked_sources as (
    select *,
           row_number() over (partition by forecast_run_id, product_id
             order by transferable_surplus_units desc, warehouse_id, warehouse_key) as source_rank,
           sum(transferable_surplus_units) over (partition by forecast_run_id, product_id
             order by transferable_surplus_units desc, warehouse_id, warehouse_key
             rows between unbounded preceding and current row) as source_end_units
    from source_surplus
    where transferable_surplus_units > 0
), source_ranges as (
    select *, source_end_units - transferable_surplus_units as source_start_units
    from ranked_sources
), ranked_destinations as (
    select *,
           row_number() over (partition by forecast_run_id, product_id
             order by case stockout_risk_tier when 'CRITICAL' then 1 when 'HIGH' then 2 else 3 end,
                      expected_shortage_units desc, warehouse_id, warehouse_key) as destination_rank,
           sum(transfer_need_units) over (partition by forecast_run_id, product_id
             order by case stockout_risk_tier when 'CRITICAL' then 1 when 'HIGH' then 2 else 3 end,
                      expected_shortage_units desc, warehouse_id, warehouse_key
             rows between unbounded preceding and current row) as destination_end_units
    from destination_need
    where transfer_need_units > 0
), destination_ranges as (
    select *, destination_end_units - transfer_need_units as destination_start_units
    from ranked_destinations
), allocation as (
    select
        s.forecast_run_id,
        to_date('2024-12-31') as planning_cutoff,
        s.product_id,
        s.warehouse_id as source_warehouse_id,
        s.warehouse_key as source_warehouse_key,
        d.warehouse_id as destination_warehouse_id,
        d.warehouse_key as destination_warehouse_key,
        s.source_rank,
        d.destination_rank,
        s.source_available_units,
        s.source_protected_requirement_units,
        s.transferable_surplus_units,
        d.expected_shortage_units as destination_expected_shortage_units,
        d.transfer_need_units as destination_transfer_need_units,
        greatest(least(s.source_end_units, d.destination_end_units)
          - greatest(s.source_start_units, d.destination_start_units), 0) as transfer_units,
        s.source_inventory_snapshot_key,
        s.source_selected_po_line_id,
        d.inventory_snapshot_key as destination_inventory_snapshot_key,
        d.selected_po_line_id as destination_selected_po_line_id,
        d.selected_supplier_key as destination_supplier_key,
        d.stockout_risk_tier as destination_risk_tier
    from source_ranges s
    join destination_ranges d
      on s.forecast_run_id = d.forecast_run_id
     and s.product_id = d.product_id
     and s.warehouse_id <> d.warehouse_id
)
select *
from allocation
where transfer_units > 0
