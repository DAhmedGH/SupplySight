{{ config(materialized='table') }}

-- Preserves the CORE inventory snapshot grain and keys for traceable analysis.
select
    i.inventory_snapshot_key,
    i.snapshot_date_key,
    i.snapshot_date,
    i.warehouse_key,
    i.warehouse_id,
    i.product_key,
    p.product_id,
    p.sku,
    p.product_name,
    p.category,
    p.reorder_point,
    i.on_hand_units,
    i.reserved_units,
    i.available_units,
    i.in_transit_units,
    i.inventory_value,
    greatest(i.available_units - coalesce(p.reorder_point, 0), 0) as excess_units,
    i.available_units <= 0 as is_stockout,
    i.quality_status,
    i.quality_issues,
    i.product_history_fallback,
    i.batch_id,
    i.source_file,
    i.source_system,
    i.row_hash
from {{ ref('fact_inventory_snapshot') }} i
join {{ ref('dim_product') }} p on i.product_key = p.product_key
