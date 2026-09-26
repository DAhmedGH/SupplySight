{{ config(materialized='table') }}
select md5(x.shipment_id) as shipment_fact_key, x.shipment_id, x.order_line_id,
       x.order_id, d.date_key as ship_date_key, dd.date_key as delivery_date_key, x.delivery_date,
       c.customer_key, w.warehouse_key, ca.carrier_key, x.selected_product_key as product_key,
       x.product_history_fallback,
       x.shipped_quantity, x.shipment_status, x.carrier,
       x.quality_status, x.quality_issues,
       x.upstream_order_quality_status, x.upstream_order_quality_issues, x.upstream_order_batch_id,
       x.ingested_at, x.source_file, x.batch_id, x.source_system, x.row_hash
from {{ ref('int_shipment_eligibility') }} x
join {{ ref('stg_orders') }} o on x.order_line_id = o.order_line_id
join {{ ref('dim_date') }} d on x.ship_date = d.date_day
left join {{ ref('dim_date') }} dd on x.delivery_date = dd.date_day
join {{ ref('dim_warehouse') }} w on x.warehouse_id = w.warehouse_id
join {{ ref('dim_customer') }} c on o.customer_id = c.customer_id
join {{ ref('dim_carrier') }} ca on upper(trim(x.carrier)) = ca.carrier_name
where x.eligible_for_fact
