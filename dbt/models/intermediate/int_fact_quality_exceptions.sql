{{ config(materialized='view') }}
select 'orders' as source_entity, order_line_id as source_key, quality_status, quality_issues,
       exclusion_reason, selected_product_quality_status, ingested_at, source_file, batch_id, source_system, row_hash
from {{ ref('int_order_eligibility') }} where not eligible_for_fact
union all
select 'inventory_snapshots', concat_ws('|', snapshot_date, warehouse_id, product_id), quality_status, quality_issues,
       exclusion_reason, selected_product_quality_status, ingested_at, source_file, batch_id, source_system, row_hash
from {{ ref('int_inventory_eligibility') }} where not eligible_for_fact
union all
select 'purchase_orders', po_line_id, quality_status, quality_issues,
       exclusion_reason, selected_product_quality_status, ingested_at, source_file, batch_id, source_system, row_hash
from {{ ref('int_purchase_order_eligibility') }} where not eligible_for_fact
union all
select 'shipments', shipment_id, quality_status, quality_issues,
       exclusion_reason, selected_product_quality_status, ingested_at, source_file, batch_id, source_system, row_hash
from {{ ref('int_shipment_eligibility') }} where not eligible_for_fact
union all
select 'returns', return_id, quality_status, quality_issues,
       exclusion_reason, selected_product_quality_status, ingested_at, source_file, batch_id, source_system, row_hash
from {{ ref('int_return_eligibility') }} where not eligible_for_fact
