{% snapshot snap_products %}
{{
    config(
      target_schema='INTERMEDIATE',
      unique_key='product_id',
      strategy='check',
      check_cols=['sku', 'product_name', 'category', 'subcategory', 'unit_cost', 'list_price', 'launch_date', 'active_flag', 'reorder_point', 'quality_status', 'quality_issues'],
      invalidate_hard_deletes=False
    )
}}
select product_id, sku, product_name, category, subcategory, unit_cost, list_price,
       launch_date, active_flag, reorder_point, quality_status, quality_issues,
       is_quarantined, ingested_at, source_file, batch_id, source_system, row_hash
from {{ ref('stg_products') }}
{% endsnapshot %}
