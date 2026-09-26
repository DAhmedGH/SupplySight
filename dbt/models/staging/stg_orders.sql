with ranked as (
    select *, row_number() over (partition by trim(order_line_id) order by _ingested_at desc, _batch_id desc, _row_hash desc) as _dbt_row_number
    from {{ source('raw', 'orders') }}
)
select
    nullif(trim(order_id), '') as order_id, nullif(trim(order_line_id), '') as order_line_id,
    try_to_date(nullif(trim(order_date), '')) as order_date,
    nullif(trim(customer_id), '') as customer_id, nullif(trim(product_id), '') as product_id,
    nullif(trim(warehouse_id), '') as warehouse_id,
    try_to_number(nullif(trim(quantity), ''), 38, 0) as quantity,
    try_to_decimal(nullif(trim(unit_price), ''), 38, 18) as unit_price,
    try_to_decimal(nullif(trim(discount_pct), ''), 38, 18) as discount_pct,
    lower(nullif(trim(status), '')) as status,
    try_to_date(nullif(trim(promised_delivery_date), '')) as promised_delivery_date,
    _ingested_at as ingested_at, _source_file as source_file, _batch_id as batch_id,
    _source_system as source_system, _row_hash as row_hash,
    upper(_quality_status) as quality_status, coalesce(_quality_issues, array_construct()) as quality_issues,
    upper(_quality_status) = 'QUARANTINED' as is_quarantined
from ranked where _dbt_row_number = 1
