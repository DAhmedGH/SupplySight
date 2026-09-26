with source_rows as (
    select * from {{ source('raw', 'products') }}
), ranked as (
    select *, row_number() over (partition by trim(product_id) order by _ingested_at desc, _batch_id desc, _row_hash desc) as _dbt_row_number
    from source_rows
)
select
    nullif(trim(product_id), '') as product_id,
    nullif(trim(sku), '') as sku,
    nullif(trim(product_name), '') as product_name,
    lower(nullif(trim(category), '')) as category,
    nullif(trim(subcategory), '') as subcategory,
    try_to_decimal(nullif(trim(unit_cost), ''), 38, 18) as unit_cost,
    try_to_decimal(nullif(trim(list_price), ''), 38, 18) as list_price,
    try_to_date(nullif(trim(launch_date), '')) as launch_date,
    try_to_boolean(nullif(trim(active_flag), '')) as active_flag,
    try_to_number(nullif(trim(reorder_point), ''), 38, 0) as reorder_point,
    _ingested_at as ingested_at, _source_file as source_file, _batch_id as batch_id,
    _source_system as source_system, _row_hash as row_hash,
    upper(_quality_status) as quality_status, coalesce(_quality_issues, array_construct()) as quality_issues,
    upper(_quality_status) = 'QUARANTINED' as is_quarantined
from ranked where _dbt_row_number = 1
