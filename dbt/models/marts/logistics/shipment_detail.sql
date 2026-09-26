{{ config(materialized='table') }}

select
    s.shipment_fact_key,
    s.shipment_id,
    s.order_line_id,
    s.order_id,
    s.ship_date_key,
    s.delivery_date_key,
    s.delivery_date,
    s.customer_key,
    s.warehouse_key,
    s.carrier_key,
    s.product_key,
    s.product_history_fallback,
    s.shipped_quantity,
    s.shipment_status,
    o.promised_delivery_date,
    s.delivery_date is not null as is_completed,
    datediff('day', to_date(to_varchar(s.ship_date_key), 'YYYYMMDD'), s.delivery_date) as transit_days,
    case when s.delivery_date is not null and o.promised_delivery_date is not null
         then s.delivery_date > o.promised_delivery_date end as is_late,
    case when s.delivery_date is not null and o.promised_delivery_date is not null
         then 1 else 0 end as timeliness_eligible_count,
    case when s.delivery_date is not null and o.promised_delivery_date is not null
              and s.delivery_date <= o.promised_delivery_date then 1 else 0 end as on_time_shipment_count,
    s.quality_status,
    s.quality_issues,
    s.upstream_order_quality_status,
    s.upstream_order_quality_issues,
    s.upstream_order_batch_id,
    (o.order_line_id is not null) as has_eligible_order_context,
    s.batch_id,
    s.source_file,
    s.source_system,
    s.row_hash,
    s.ingested_at
from {{ ref('fact_shipments') }} s
left join {{ ref('fact_orders') }} o on s.order_line_id = o.order_line_id
