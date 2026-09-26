{{ config(materialized='table') }}

-- Grain is supplier and calendar month. Spend uses PO order month; service
-- metrics use expected-delivery due month, so the cohorts are explicit.
with detail as (
    select * from {{ ref('supplier_detail') }}
),
spend as (
    select supplier_key, date_trunc('month', order_date)::date as month_start,
           sum(supplier_spend) as supplier_spend,
           count(*) as purchase_order_line_count,
           sum(supplier_spend) as supplier_spend_numerator,
           count(*) as supplier_spend_line_denominator,
           count_if(quality_status = 'WARNING') as warning_po_line_count,
           count_if(product_history_fallback) as product_history_fallback_po_line_count,
           max(as_of_date) as as_of_date
    from (
        select d.*, c.date_day as order_date
        from detail d join {{ ref('dim_date') }} c on d.order_date_key = c.date_key
    ) x
    group by supplier_key, date_trunc('month', order_date)
),
service as (
    select supplier_key, date_trunc('month', expected_delivery_date)::date as month_start,
           sum(otif_numerator) as otif_numerator,
           sum(otif_denominator) as otif_denominator,
           sum(fill_rate_numerator) as fill_rate_numerator,
           sum(fill_rate_denominator) as fill_rate_denominator,
           sum(is_late_as_of_cutoff) as late_purchase_order_count,
           count_if(is_due_as_of_cutoff) as due_purchase_order_count,
           avg(iff(received_date is not null, datediff(day, order_date, received_date), null)) as observed_avg_lead_time_days,
           stddev_samp(iff(received_date is not null, datediff(day, order_date, received_date), null)) as observed_lead_time_stddev_days,
           sum(iff(received_date is not null, datediff(day, order_date, received_date), 0)) as lead_time_days_sum,
           count_if(received_date is not null) as lead_time_observation_count,
           count_if(quality_status = 'WARNING') as service_warning_line_count,
           count_if(product_history_fallback) as service_fallback_line_count,
           max(as_of_date) as as_of_date
    from (
        select d.*, od.date_day as order_date
        from detail d join {{ ref('dim_date') }} od on d.order_date_key = od.date_key
        where d.expected_delivery_date is not null
    ) x
    group by supplier_key, date_trunc('month', expected_delivery_date)
),
month_keys as (
    select supplier_key, month_start from spend
    union
    select supplier_key, month_start from service
)
select
    md5(concat_ws('|', k.supplier_key, k.month_start)) as supplier_monthly_key,
    k.supplier_key,
    s.supplier_id,
    k.month_start,
    sp.supplier_spend,
    sp.purchase_order_line_count,
    sv.otif_numerator,
    sv.otif_denominator,
    sv.otif_numerator / nullif(sv.otif_denominator, 0)::float as supplier_otif_rate,
    sv.fill_rate_numerator,
    sv.fill_rate_denominator,
    sv.fill_rate_numerator / nullif(sv.fill_rate_denominator, 0)::float as supplier_fill_rate,
    sv.late_purchase_order_count,
    sv.due_purchase_order_count,
    sv.lead_time_days_sum / nullif(sv.lead_time_observation_count, 0)::float as observed_avg_lead_time_days,
    sv.observed_lead_time_stddev_days,
    sv.lead_time_days_sum as lead_time_numerator_days,
    sv.lead_time_observation_count as lead_time_denominator_count,
    sp.supplier_spend_numerator,
    sp.supplier_spend_line_denominator,
    sp.warning_po_line_count,
    sp.product_history_fallback_po_line_count,
    sv.service_warning_line_count,
    sv.service_fallback_line_count,
    coalesce(sv.as_of_date, sp.as_of_date) as as_of_date
from month_keys k
join {{ ref('dim_supplier') }} s on k.supplier_key = s.supplier_key
left join spend sp on k.supplier_key = sp.supplier_key and k.month_start = sp.month_start
left join service sv on k.supplier_key = sv.supplier_key and k.month_start = sv.month_start
