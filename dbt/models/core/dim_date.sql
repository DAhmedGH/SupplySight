{{ config(materialized='table') }}
with calendar as (
  select dateadd(day, row_number() over (order by seq4()) - 1, to_date('2000-01-01'))::date as date_day
  from table(generator(rowcount => 18628))
)
select to_number(to_char(date_day, 'YYYYMMDD'))::number(8,0) as date_key,
       date_day, year(date_day) as year_number, quarter(date_day) as quarter_number,
       month(date_day) as month_number, monthname(date_day) as month_name,
       day(date_day) as day_of_month, dayofweekiso(date_day) as day_of_week_iso,
       dayname(date_day) as day_name, weekiso(date_day) as week_of_year,
       date_trunc('month', date_day)::date as month_start_date,
       date_trunc('quarter', date_day)::date as quarter_start_date,
       date_trunc('year', date_day)::date as year_start_date,
       dayofweekiso(date_day) in (6,7) as is_weekend
from calendar
