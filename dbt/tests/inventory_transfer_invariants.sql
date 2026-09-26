{{ config(tags=['inventory_intelligence']) }}

with source_totals as (
    select forecast_run_id, product_id, source_warehouse_key,
           sum(transfer_units) as allocated_units,
           max(transferable_surplus_units) as transferable_surplus_units,
           max(source_available_units) as source_available_units,
           max(source_protected_requirement_units) as protected_requirement_units
    from {{ ref('mart_inventory_transfers') }}
    group by forecast_run_id, product_id, source_warehouse_key
), destination_totals as (
    select forecast_run_id, product_id, destination_warehouse_key,
           sum(transfer_units) as allocated_units,
           max(destination_expected_shortage_units) as expected_shortage_units
    from {{ ref('mart_inventory_transfers') }}
    group by forecast_run_id, product_id, destination_warehouse_key
), allocation_failures as (
    select 'source_allocation_exceeds_surplus_or_protection' as failure,
           concat_ws('|', forecast_run_id, product_id, source_warehouse_key) as detail
    from source_totals
    where allocated_units > transferable_surplus_units
       or source_available_units - allocated_units < protected_requirement_units
    union all
    select 'destination_allocation_exceeds_need',
           concat_ws('|', forecast_run_id, product_id, destination_warehouse_key)
    from destination_totals
    where allocated_units > expected_shortage_units
)
select 'nonpositive_or_fractional_transfer' as failure,
       concat_ws('|', forecast_run_id, product_id, source_warehouse_key, destination_warehouse_key) as detail
from {{ ref('mart_inventory_transfers') }}
where transfer_units <= 0 or transfer_units <> floor(transfer_units)
union all select * from allocation_failures
