select ingestion_month, source_entity, exclusion_reason,
       count(*) as excluded_record_count,
       count_if(quality_status = 'QUARANTINED') as quarantined_record_count,
       count_if(quality_status = 'WARNING') as warning_record_count,
       count_if(selected_product_quality_status = 'QUARANTINED') as quarantined_product_record_count
from {{ ref('mart_quality_exceptions') }}
group by ingestion_month, source_entity, exclusion_reason
