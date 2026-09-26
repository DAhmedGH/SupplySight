with ranked as (
    select *, row_number() over (partition by trim(customer_id) order by _ingested_at desc, _batch_id desc, _row_hash desc) as _dbt_row_number
    from {{ source('raw', 'customers') }}
)
select
    nullif(trim(customer_id), '') as customer_id,
    lower(nullif(trim(customer_segment), '')) as customer_segment,
    upper(nullif(trim(region), '')) as region,
    upper(nullif(trim(country), '')) as country,
    try_to_date(nullif(trim(signup_date), '')) as signup_date,
    _ingested_at as ingested_at, _source_file as source_file, _batch_id as batch_id,
    _source_system as source_system, _row_hash as row_hash,
    upper(_quality_status) as quality_status, coalesce(_quality_issues, array_construct()) as quality_issues,
    upper(_quality_status) = 'QUARANTINED' as is_quarantined
from ranked where _dbt_row_number = 1
