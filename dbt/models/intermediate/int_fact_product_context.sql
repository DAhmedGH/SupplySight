{{ config(materialized='view') }}
with events as (
  select 'orders' as source_entity, order_line_id as source_key, product_id, order_date as event_date from {{ ref('stg_orders') }}
  union all select 'inventory_snapshots', concat_ws('|', snapshot_date, warehouse_id, product_id), product_id, snapshot_date from {{ ref('stg_inventory_snapshots') }}
  union all select 'purchase_orders', po_line_id, product_id, order_date from {{ ref('stg_purchase_orders') }}
  union all select 'shipments', s.shipment_id, o.product_id, s.ship_date
    from {{ ref('stg_shipments') }} s left join {{ ref('stg_orders') }} o on s.order_line_id = o.order_line_id
  union all select 'returns', r.return_id, o.product_id, r.return_date
    from {{ ref('stg_returns') }} r left join {{ ref('stg_orders') }} o on r.order_line_id = o.order_line_id
), candidates as (
  select e.source_entity, e.source_key, p.product_id, p.product_key, p.quality_status,
         p.valid_from, p.valid_to, e.event_date,
         (p.valid_from < dateadd(day, 1, e.event_date)::timestamp_ntz
           and (p.valid_to is null or p.valid_to >= dateadd(day, 1, e.event_date)::timestamp_ntz)) as event_version_matches,
         row_number() over (partition by e.source_entity, e.source_key order by
           case when p.valid_from < dateadd(day, 1, e.event_date)::timestamp_ntz
             and (p.valid_to is null or p.valid_to >= dateadd(day, 1, e.event_date)::timestamp_ntz) then 0 else 1 end,
           case when p.valid_from < dateadd(day, 1, e.event_date)::timestamp_ntz
             and (p.valid_to is null or p.valid_to >= dateadd(day, 1, e.event_date)::timestamp_ntz) then p.valid_from end desc,
           p.valid_from asc) as candidate_number
  from events e
  left join {{ ref('dim_product') }} p on e.product_id = p.product_id
)
select source_entity, source_key, event_date, product_id, product_key as selected_product_key,
       valid_from as selected_product_valid_from, valid_to as selected_product_valid_to,
       quality_status as selected_product_quality_status,
       not coalesce(event_version_matches, false) as product_history_fallback
from candidates where candidate_number = 1
