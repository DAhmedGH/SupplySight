{{ config(tags=['marts']) }}

select 'sales_recognition' as failure, to_varchar(order_line_id) as source_key
from {{ ref('sales_order_line') }}
where (order_status = 'delivered' and
       (revenue_amount is distinct from booked_order_amount
        or cost_of_goods_sold is distinct from quantity * unit_cost))
   or (order_status <> 'delivered' and
       (revenue_amount <> 0 or cost_of_goods_sold <> 0 or gross_profit_amount <> 0))
union all
select 'executive_gross_profit', to_varchar(month_start)
from {{ ref('mart_executive_monthly') }}
where abs(gross_profit_amount - (revenue_amount - cost_of_goods_sold)) > 0.0001
union all
select 'executive_ratio_bounds', to_varchar(month_start)
from {{ ref('mart_executive_monthly') }}
where (stockout_rate is not null and (stockout_rate < 0 or stockout_rate > 1))
   or (supplier_otif_rate is not null and (supplier_otif_rate < 0 or supplier_otif_rate > 1))
   or (supplier_fill_rate is not null and (supplier_fill_rate < 0 or supplier_fill_rate > 1))
   or (on_time_delivery_rate is not null and (on_time_delivery_rate < 0 or on_time_delivery_rate > 1))
   or stockout_snapshot_count > inventory_snapshot_count
   or supplier_otif_numerator > supplier_otif_denominator
   or supplier_fill_numerator > supplier_fill_denominator
   or on_time_shipment_count > timeliness_eligible_count
