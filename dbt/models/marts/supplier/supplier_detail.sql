{{ config(materialized='table') }}

-- Grain is one eligible CORE purchase-order line. Expected-date service metrics
-- remain null until the due date is in the observed data cutoff.
with cutoff as (
    select max(d.date_day) as as_of_date
    from {{ ref('fact_purchase_orders') }} po
    join {{ ref('dim_date') }} d on po.order_date_key = d.date_key
)
select
    po.purchase_order_fact_key,
    po.po_line_id,
    po.purchase_order_id,
    po.supplier_key,
    s.supplier_id,
    po.warehouse_key,
    po.product_key,
    p.product_id,
    po.order_date_key,
    po.expected_delivery_date_key,
    po.received_date_key,
    po.expected_delivery_date,
    po.received_date,
    po.ordered_quantity,
    po.received_quantity,
    po.unit_cost,
    po.ordered_cost as supplier_spend,
    po.status,
    po.expected_delivery_date <= c.as_of_date as is_due_as_of_cutoff,
    iff(po.expected_delivery_date <= c.as_of_date,
        iff(po.received_date is not null and po.received_date <= po.expected_delivery_date
            and po.received_quantity >= po.ordered_quantity, 1, 0), null) as otif_numerator,
    iff(po.expected_delivery_date <= c.as_of_date, 1, null) as otif_denominator,
    iff(po.expected_delivery_date <= c.as_of_date,
        least(po.received_quantity, po.ordered_quantity), null) as fill_rate_numerator,
    iff(po.expected_delivery_date <= c.as_of_date, po.ordered_quantity, null) as fill_rate_denominator,
    iff(po.expected_delivery_date < c.as_of_date
        and (po.received_date is null or po.received_date > po.expected_delivery_date
             or po.received_quantity < po.ordered_quantity), 1, 0) as is_late_as_of_cutoff,
    po.quality_status,
    po.quality_issues,
    po.product_history_fallback,
    po.batch_id,
    po.source_file,
    po.source_system,
    po.row_hash,
    c.as_of_date
from {{ ref('fact_purchase_orders') }} po
join {{ ref('dim_supplier') }} s on po.supplier_key = s.supplier_key
join {{ ref('dim_product') }} p on po.product_key = p.product_key
cross join cutoff c
