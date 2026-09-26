with ranked as (
    select *, row_number() over (partition by trim(po_line_id) order by _ingested_at desc, _batch_id desc, _row_hash desc) as _dbt_row_number
    from {{ source('raw', 'purchase_orders') }}
)
select
    nullif(trim(purchase_order_id), '') as purchase_order_id, nullif(trim(po_line_id), '') as po_line_id,
    nullif(trim(supplier_id), '') as supplier_id, nullif(trim(warehouse_id), '') as warehouse_id,
    nullif(trim(product_id), '') as product_id,
    try_to_date(nullif(trim(order_date), '')) as order_date,
    try_to_date(nullif(trim(expected_delivery_date), '')) as expected_delivery_date,
    try_to_date(nullif(trim(received_date), '')) as received_date,
    try_to_number(nullif(trim(ordered_quantity), ''), 38, 0) as ordered_quantity,
    try_to_number(nullif(trim(received_quantity), ''), 38, 0) as received_quantity,
    try_to_decimal(nullif(trim(unit_cost), ''), 38, 18) as unit_cost,
    lower(nullif(trim(status), '')) as status,
    _ingested_at as ingested_at, _source_file as source_file, _batch_id as batch_id,
    _source_system as source_system, _row_hash as row_hash,
    upper(_quality_status) as quality_status, coalesce(_quality_issues, array_construct()) as quality_issues,
    upper(_quality_status) = 'QUARANTINED' as is_quarantined
from ranked where _dbt_row_number = 1
