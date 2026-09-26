{{ config(materialized='table') }}
select md5(x.return_id) as return_fact_key, x.return_id, x.order_line_id, x.order_id,
       d.date_key as return_date_key, c.customer_key, w.warehouse_key, x.selected_product_key as product_key,
       x.product_history_fallback,
       x.quantity as returned_quantity, x.refund_amount, x.reason, x.disposition,
       x.quality_status, x.quality_issues,
       x.upstream_order_quality_status, x.upstream_order_quality_issues, x.upstream_order_batch_id,
       x.ingested_at, x.source_file, x.batch_id, x.source_system, x.row_hash
from {{ ref('int_return_eligibility') }} x
join {{ ref('dim_date') }} d on x.return_date = d.date_day
join {{ ref('dim_customer') }} c on x.customer_id = c.customer_id
join {{ ref('dim_warehouse') }} w on x.warehouse_id = w.warehouse_id
where x.eligible_for_fact
