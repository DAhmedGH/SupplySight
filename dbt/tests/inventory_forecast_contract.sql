{{ config(tags=['inventory_intelligence']) }}

with forecasts as (
    select * from {{ source('forecasting', 'current_forecasts') }}
), metadata as (
    select * from {{ source('forecasting', 'run_metadata') }}
), run_counts as (
    select count(distinct f.run_id) as current_run_count,
           count_if(m.run_id is null or upper(m.status) <> 'SUCCEEDED'
                    or m.observed_through <> to_date('2024-12-31')
                    or m.horizon <> 3) as incompatible_run_count
    from forecasts f
    left join metadata m on f.run_id = m.run_id
), series_checks as (
    select run_id, product_id, warehouse_key,
           count(*) as row_count,
           count(distinct forecast_date) as distinct_month_count,
           count_if(forecast_date not in (to_date('2025-01-01'), to_date('2025-02-01'), to_date('2025-03-01'))) as unexpected_month_count,
           count_if(upper_bound is null or upper_bound < predicted_demand) as invalid_bound_count,
           min(forecast_date) as min_month,
           max(forecast_date) as max_month
    from forecasts
    group by run_id, product_id, warehouse_key
), series_failures as (
    select * from series_checks
    where row_count <> 3 or distinct_month_count <> 3 or unexpected_month_count <> 0
       or invalid_bound_count <> 0
       or min_month <> to_date('2025-01-01') or max_month <> to_date('2025-03-01')
), metadata_counts as (
    select f.run_id,
           count(*) as actual_record_count,
           count(distinct concat_ws('|', f.product_id, f.warehouse_key)) as actual_series_count
    from forecasts f
    group by f.run_id
), metadata_failures as (
    select 'forecast_run_metadata_count_mismatch' as failure, to_varchar(m.run_id) as detail
    from metadata_counts c
    join metadata m on c.run_id = m.run_id
    where c.actual_record_count <> m.records_produced
       or c.actual_series_count <> m.series_succeeded
)
select 'current_forecast_run_count_or_compatibility' as failure,
       to_varchar(current_run_count) as detail
from run_counts
where current_run_count <> 1 or incompatible_run_count > 0
union all
select 'forecast_series_horizon_or_bound' as failure,
       concat_ws('|', run_id, product_id, warehouse_key)
from series_failures
union all
select failure, detail from metadata_failures
