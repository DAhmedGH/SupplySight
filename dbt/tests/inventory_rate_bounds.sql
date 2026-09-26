select *
from {{ ref('inventory_monthly') }}
where stockout_rate < 0 or stockout_rate > 1
   or stockout_snapshot_count > snapshot_count
   or avg_available_units is null
