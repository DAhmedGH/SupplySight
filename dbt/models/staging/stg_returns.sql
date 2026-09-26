with ranked as (
    select *, row_number() over (partition by trim(return_id) order by _ingested_at desc, _batch_id desc, _row_hash desc) as _dbt_row_number
    from {{ source('raw', 'returns') }}
)
select
    nullif(trim(return_id), '') as return_id, nullif(trim(order_line_id), '') as order_line_id,
    try_to_date(nullif(trim(return_date), '')) as return_date,
    try_to_number(nullif(trim(quantity), ''), 38, 0) as quantity,
    lower(nullif(trim(reason), '')) as reason,
    lower(nullif(trim(disposition), '')) as disposition,
    try_to_decimal(nullif(trim(refund_amount), ''), 38, 18) as refund_amount,
    _ingested_at as ingested_at, _source_file as source_file, _batch_id as batch_id,
    _source_system as source_system, _row_hash as row_hash,
    upper(_quality_status) as quality_status, coalesce(_quality_issues, array_construct()) as quality_issues,
    upper(_quality_status) = 'QUARANTINED' as is_quarantined
from ranked where _dbt_row_number = 1
