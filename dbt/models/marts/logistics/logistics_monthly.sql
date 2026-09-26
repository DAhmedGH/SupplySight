{{ config(materialized='table') }}

select
    date_trunc('month', to_date(to_varchar(ship_date_key), 'YYYYMMDD'))::date as ship_month,
    carrier_key,
    warehouse_key,
    count(*) as shipment_count,
    sum(shipped_quantity) as shipped_quantity,
    count_if(is_completed) as completed_shipment_count,
    sum(timeliness_eligible_count) as timeliness_eligible_count,
    sum(on_time_shipment_count) as on_time_shipment_count,
    sum(case when is_late then 1 else 0 end) as late_shipment_count,
    avg(transit_days) as average_transit_days,
    stddev_samp(transit_days) as transit_days_stddev,
    count_if(quality_status = 'WARNING') as warning_shipment_count,
    count_if(upstream_order_quality_status = 'WARNING') as upstream_warning_order_count,
    count_if(upstream_order_quality_status = 'QUARANTINED') as upstream_quarantined_order_count,
    count_if(product_history_fallback) as product_history_fallback_shipment_count
from {{ ref('shipment_detail') }}
group by 1, 2, 3
