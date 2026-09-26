{{ config(materialized='table') }}

select
    date_trunc('month', to_date(to_varchar(r.return_date_key), 'YYYYMMDD'))::date as return_month,
    r.customer_key,
    r.product_key,
    r.warehouse_key,
    count(*) as return_count,
    sum(r.returned_quantity) as returned_quantity,
    sum(r.refund_amount) as refund_amount,
    count_if(r.quality_status = 'WARNING') as warning_return_count,
    count_if(r.upstream_order_quality_status = 'WARNING') as upstream_warning_order_count,
    count_if(r.upstream_order_quality_status = 'QUARANTINED') as upstream_quarantined_order_count,
    count_if(r.product_history_fallback) as product_history_fallback_return_count
from {{ ref('fact_returns') }} r
group by 1, 2, 3, 4
