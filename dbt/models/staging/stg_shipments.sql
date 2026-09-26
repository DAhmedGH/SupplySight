with ranked as (
    select *, row_number() over (partition by trim(shipment_id) order by _ingested_at desc, _batch_id desc, _row_hash desc) as _dbt_row_number
    from {{ source('raw', 'shipments') }}
)
select
    nullif(trim(shipment_id), '') as shipment_id, nullif(trim(order_line_id), '') as order_line_id,
    nullif(trim(warehouse_id), '') as warehouse_id,
    try_to_date(nullif(trim(ship_date), '')) as ship_date,
    try_to_date(nullif(trim(delivery_date), '')) as delivery_date,
    try_to_number(nullif(trim(shipped_quantity), ''), 38, 0) as shipped_quantity,
    lower(nullif(trim(shipment_status), '')) as shipment_status,
    nullif(trim(carrier), '') as carrier,
    _ingested_at as ingested_at, _source_file as source_file, _batch_id as batch_id,
    _source_system as source_system, _row_hash as row_hash,
    upper(_quality_status) as quality_status, coalesce(_quality_issues, array_construct()) as quality_issues,
    upper(_quality_status) = 'QUARANTINED' as is_quarantined
from ranked where _dbt_row_number = 1
