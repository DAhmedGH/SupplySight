select *
from {{ ref('supplier_monthly') }}
where supplier_otif_rate < 0 or supplier_otif_rate > 1
   or supplier_fill_rate < 0 or supplier_fill_rate > 1
   or otif_numerator > otif_denominator
   or fill_rate_numerator > fill_rate_denominator
