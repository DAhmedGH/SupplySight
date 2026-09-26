{{ config(materialized='table') }}
select md5(upper(trim(carrier))) as carrier_key,
       upper(trim(carrier)) as carrier_name
from {{ ref('stg_shipments') }}
where nullif(trim(carrier), '') is not null and quality_status <> 'QUARANTINED'
group by upper(trim(carrier))
