{{ config(materialized='table') }}
select md5(x.po_line_id) as purchase_order_fact_key, x.po_line_id, x.purchase_order_id,
       d.date_key as order_date_key, ed.date_key as expected_delivery_date_key,
       rd.date_key as received_date_key, s.supplier_key, w.warehouse_key, x.selected_product_key as product_key,
       x.product_history_fallback,
       x.expected_delivery_date, x.received_date,
       x.ordered_quantity, x.received_quantity, x.unit_cost,
       x.ordered_quantity * x.unit_cost as ordered_cost,
       x.status, x.quality_status, x.quality_issues, x.ingested_at, x.source_file,
       x.batch_id, x.source_system, x.row_hash
from {{ ref('int_purchase_order_eligibility') }} x
join {{ ref('dim_date') }} d on x.order_date = d.date_day
left join {{ ref('dim_date') }} ed on x.expected_delivery_date = ed.date_day
left join {{ ref('dim_date') }} rd on x.received_date = rd.date_day
join {{ ref('dim_supplier') }} s on x.supplier_id = s.supplier_id
join {{ ref('dim_warehouse') }} w on x.warehouse_id = w.warehouse_id
where x.eligible_for_fact
