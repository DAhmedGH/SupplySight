{{ config(materialized='view') }}

-- PO source is period-end state, not status-event history. Keep post-cutoff orders
-- visible but explicitly unplanned; snapshot in_transit_units is never added here.
with forecast_runs as (
    select distinct f.run_id, m.status, m.observed_through, m.horizon,
           m.series_succeeded, m.records_produced, m.source_version, m.model_version
    from {{ source('forecasting', 'current_forecasts') }} f
    join {{ source('forecasting', 'run_metadata') }} m on f.run_id = m.run_id
    where upper(m.status) = 'SUCCEEDED'
), po as (
    select s.*, d.date_day as order_date
    from {{ ref('supplier_detail') }} s
    left join {{ ref('dim_date') }} d on s.order_date_key = d.date_key
)
select
    r.run_id as forecast_run_id,
    to_date('2024-12-31') as planning_cutoff,
    p.po_line_id,
    p.purchase_order_id,
    p.product_id,
    p.product_key,
    p.warehouse_key,
    w.warehouse_id,
    p.supplier_key,
    p.supplier_id,
    p.order_date,
    p.expected_delivery_date,
    p.received_date,
    p.ordered_quantity,
    p.received_quantity,
    greatest(p.ordered_quantity - p.received_quantity, 0) as remaining_units,
    datediff(day, p.order_date, p.expected_delivery_date) as planned_lead_time_days,
    p.status as po_status,
    case
      when p.order_date > to_date('2024-12-31') then 'FUTURE_ORDER_EXCLUDED'
      when p.received_date > to_date('2024-12-31') then 'POST_CUTOFF_RECEIPT_DATE_EXCLUDED'
      when p.status = 'in_transit' and p.received_date is null
       and p.received_quantity = 0 and p.ordered_quantity > p.received_quantity
       and p.expected_delivery_date > to_date('2024-12-31') then 'SCHEDULED_INBOUND'
      when p.status = 'in_transit' and p.received_date is null
       and p.received_quantity = 0 and p.ordered_quantity > p.received_quantity
       and p.expected_delivery_date <= to_date('2024-12-31') then 'OVERDUE_UNCERTAIN'
      when p.status = 'open' and p.order_date <= to_date('2024-12-31')
       and p.received_date is null and p.received_quantity = 0
       and p.ordered_quantity > p.received_quantity then 'OPEN_UNCERTAIN'
      else 'NOT_PLANNED'
    end as inbound_classification,
    iff(p.status = 'in_transit' and p.received_date is null
        and p.received_quantity = 0 and p.ordered_quantity > p.received_quantity
        and p.order_date <= to_date('2024-12-31')
        and p.expected_delivery_date > to_date('2024-12-31'),
        greatest(p.ordered_quantity - p.received_quantity, 0), 0) as scheduled_inbound_units,
    p.quality_status,
    p.quality_issues,
    p.product_history_fallback,
    p.batch_id,
    p.source_file,
    p.source_system,
    p.row_hash,
    r.status as forecast_run_status,
    r.observed_through,
    r.horizon,
    r.source_version,
    r.model_version
from forecast_runs r
cross join po p
left join {{ ref('dim_warehouse') }} w on p.warehouse_key = w.warehouse_key
