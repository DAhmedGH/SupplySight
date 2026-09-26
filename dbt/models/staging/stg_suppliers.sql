with ranked as (
    select *, row_number() over (partition by trim(supplier_id) order by _ingested_at desc, _batch_id desc, _row_hash desc) as _dbt_row_number
    from {{ source('raw', 'suppliers') }}
)
select
    nullif(trim(supplier_id), '') as supplier_id,
    nullif(trim(supplier_name), '') as supplier_name,
    upper(nullif(trim(region), '')) as region,
    upper(nullif(trim(country), '')) as country,
    try_to_number(nullif(trim(lead_time_days), ''), 38, 0) as lead_time_days,
    try_to_decimal(nullif(trim(lead_time_std_days), ''), 38, 18) as lead_time_std_days,
    try_to_decimal(nullif(trim(on_time_rate), ''), 38, 18) as on_time_rate,
    try_to_decimal(nullif(trim(quality_rate), ''), 38, 18) as quality_rate,
    try_to_decimal(nullif(trim(cost_factor), ''), 38, 18) as cost_factor,
    try_to_boolean(nullif(trim(active_flag), '')) as active_flag,
    _ingested_at as ingested_at, _source_file as source_file, _batch_id as batch_id,
    _source_system as source_system, _row_hash as row_hash,
    upper(_quality_status) as quality_status, coalesce(_quality_issues, array_construct()) as quality_issues,
    upper(_quality_status) = 'QUARANTINED' as is_quarantined
from ranked where _dbt_row_number = 1
