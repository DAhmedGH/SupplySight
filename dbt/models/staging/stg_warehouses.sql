with ranked as (
    select *, row_number() over (partition by trim(warehouse_id) order by _ingested_at desc, _batch_id desc, _row_hash desc) as _dbt_row_number
    from {{ source('raw', 'warehouses') }}
)
select
    nullif(trim(warehouse_id), '') as warehouse_id,
    nullif(trim(warehouse_name), '') as warehouse_name,
    upper(nullif(trim(region), '')) as region,
    upper(nullif(trim(country), '')) as country,
    try_to_number(nullif(trim(capacity_units), ''), 38, 0) as capacity_units,
    _ingested_at as ingested_at, _source_file as source_file, _batch_id as batch_id,
    _source_system as source_system, _row_hash as row_hash,
    upper(_quality_status) as quality_status, coalesce(_quality_issues, array_construct()) as quality_issues,
    upper(_quality_status) = 'QUARANTINED' as is_quarantined
from ranked where _dbt_row_number = 1
