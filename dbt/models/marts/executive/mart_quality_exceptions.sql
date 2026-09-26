{{ config(materialized='view') }}

select source_entity, source_key, quality_status, quality_issues,
       exclusion_reason, selected_product_quality_status,
       ingested_at, date_trunc('month', ingested_at)::date as ingestion_month,
       source_file, batch_id, source_system, row_hash
from {{ ref('int_fact_quality_exceptions') }}
