with ranked as (
    select *, row_number() over (partition by try_to_date(nullif(trim(snapshot_date), '')), trim(warehouse_id), trim(product_id) order by _ingested_at desc, _batch_id desc, _row_hash desc) as _dbt_row_number
    from {{ source('raw', 'inventory_snapshots') }}
)
select
    try_to_date(nullif(trim(snapshot_date), '')) as snapshot_date,
    nullif(trim(warehouse_id), '') as warehouse_id, nullif(trim(product_id), '') as product_id,
    try_to_number(nullif(trim(on_hand_units), ''), 38, 0) as on_hand_units,
    try_to_number(nullif(trim(reserved_units), ''), 38, 0) as reserved_units,
    try_to_number(nullif(trim(available_units), ''), 38, 0) as available_units,
    try_to_number(nullif(trim(in_transit_units), ''), 38, 0) as in_transit_units,
    try_to_decimal(nullif(trim(inventory_value), ''), 38, 18) as inventory_value,
    _ingested_at as ingested_at, _source_file as source_file, _batch_id as batch_id,
    _source_system as source_system, _row_hash as row_hash,
    upper(_quality_status) as quality_status, coalesce(_quality_issues, array_construct()) as quality_issues,
    upper(_quality_status) = 'QUARANTINED' as is_quarantined
from ranked where _dbt_row_number = 1
