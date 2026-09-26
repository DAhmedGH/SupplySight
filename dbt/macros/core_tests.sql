{% test fact_eligibility_reconciliation(model, intermediate_model, key_column) %}
with expected as (
  select {{ key_column }} from {{ ref(intermediate_model) }} where eligible_for_fact
), actual as (
  select {{ key_column }} from {{ model }}
), missing as (
  select e.{{ key_column }} from expected e left join actual a using ({{ key_column }}) where a.{{ key_column }} is null
), extra as (
  select a.{{ key_column }} from actual a left join expected e using ({{ key_column }}) where e.{{ key_column }} is null
)
select 'missing_fact_key' as failure, to_varchar({{ key_column }}) as source_key from missing
union all
select 'unexpected_fact_key', to_varchar({{ key_column }}) from extra
{% endtest %}

{% test quality_exception_reconciliation(model) %}
with expected as (
  select 'orders' as source_entity, order_line_id as source_key, quality_status,
         to_json(array_sort(coalesce(quality_issues, array_construct()))) as quality_issues_json,
         exclusion_reason, selected_product_quality_status, ingested_at, source_file, batch_id, source_system, row_hash
  from {{ ref('int_order_eligibility') }} where not eligible_for_fact
  union all
  select 'inventory_snapshots', concat_ws('|', snapshot_date, warehouse_id, product_id), quality_status,
         to_json(array_sort(coalesce(quality_issues, array_construct()))), exclusion_reason,
         selected_product_quality_status, ingested_at, source_file, batch_id, source_system, row_hash
  from {{ ref('int_inventory_eligibility') }} where not eligible_for_fact
  union all
  select 'purchase_orders', po_line_id, quality_status,
         to_json(array_sort(coalesce(quality_issues, array_construct()))), exclusion_reason,
         selected_product_quality_status, ingested_at, source_file, batch_id, source_system, row_hash
  from {{ ref('int_purchase_order_eligibility') }} where not eligible_for_fact
  union all
  select 'shipments', shipment_id, quality_status,
         to_json(array_sort(coalesce(quality_issues, array_construct()))), exclusion_reason,
         selected_product_quality_status, ingested_at, source_file, batch_id, source_system, row_hash
  from {{ ref('int_shipment_eligibility') }} where not eligible_for_fact
  union all
  select 'returns', return_id, quality_status,
         to_json(array_sort(coalesce(quality_issues, array_construct()))), exclusion_reason,
         selected_product_quality_status, ingested_at, source_file, batch_id, source_system, row_hash
  from {{ ref('int_return_eligibility') }} where not eligible_for_fact
), actual as (
  select source_entity, source_key, quality_status,
         to_json(array_sort(coalesce(quality_issues, array_construct()))) as quality_issues_json,
         exclusion_reason, selected_product_quality_status, ingested_at, source_file, batch_id, source_system, row_hash
  from {{ model }}
), missing as (
  select e.* from expected e left join actual a using (source_entity, source_key) where a.source_key is null
), extra as (
  select a.* from actual a left join expected e using (source_entity, source_key) where e.source_key is null
), mismatched as (
  select e.source_entity, e.source_key from expected e join actual a using (source_entity, source_key)
  where e.quality_status is distinct from a.quality_status
     or e.quality_issues_json is distinct from a.quality_issues_json
     or e.exclusion_reason is distinct from a.exclusion_reason
     or e.selected_product_quality_status is distinct from a.selected_product_quality_status
     or e.ingested_at is distinct from a.ingested_at
     or e.source_file is distinct from a.source_file
     or e.batch_id is distinct from a.batch_id
     or e.source_system is distinct from a.source_system
     or e.row_hash is distinct from a.row_hash
)
select 'missing_exception' as failure, source_entity, source_key from missing
union all select 'unexpected_exception', source_entity, source_key from extra
union all select 'exception_metadata_mismatch', source_entity, source_key from mismatched
{% endtest %}

{% test scd2_validity(model, business_key) %}
with ordered as (
 select {{ business_key }}, valid_from, valid_to, is_current,
        lead(valid_from) over (partition by {{ business_key }} order by valid_from) as next_valid_from
 from {{ model }}
), invalid as (
 select * from ordered where (valid_to is not null and valid_to <= valid_from)
    or (next_valid_from is not null and (valid_to is null or valid_to > next_valid_from))
), current_counts as (
 select {{ business_key }} from {{ model }} group by {{ business_key }} having count_if(is_current) <> 1
)
select 'overlap_or_invalid_interval' as failure, to_varchar({{ business_key }}) as business_key from invalid
union all select 'current_version_count', to_varchar({{ business_key }}) from current_counts
{% endtest %}

{% test no_quarantined_product_keys(model) %}
select f.product_key
from {{ model }} f
join {{ ref('dim_product') }} p on f.product_key = p.product_key
where p.quality_status = 'QUARANTINED'
{% endtest %}

{% test optional_date_fk_coverage(model, date_key_pairs) %}
select * from {{ model }}
where {% for pair in date_key_pairs %}
  ({{ pair.date_column }} is not null and {{ pair.key_column }} is null){% if not loop.last %} or {% endif %}
{% endfor %}
{% endtest %}

{% test product_event_date_semantics(model) %}
with boundary_cases as (
  select column1::date as event_date, column2::timestamp_ntz as valid_from,
         column3::timestamp_ntz as valid_to, column4::boolean as expected_match
  from values
    ('2024-05-01', '2024-05-01 00:00:00', '2024-05-02 00:00:00', true),
    ('2024-05-01', '2024-05-02 00:00:00', null, false),
    ('2024-05-01', '2024-05-01 00:00:00', '2024-05-01 12:00:00', false),
    ('2024-05-01', '2024-05-01 12:00:00', null, true)
), boundary_failures as (
  select 'boundary_case' as failure from boundary_cases
  where ((valid_from < dateadd(day, 1, event_date)::timestamp_ntz
      and (valid_to is null or valid_to >= dateadd(day, 1, event_date)::timestamp_ntz)) is distinct from expected_match)
), context_failures as (
  select 'event_context_mismatch' as failure
  from {{ model }} c
  join {{ ref('dim_product') }} p on c.selected_product_key = p.product_key
  where c.event_date is not null and (
    (not c.product_history_fallback and not (
      p.valid_from < dateadd(day, 1, c.event_date)::timestamp_ntz
      and (p.valid_to is null or p.valid_to >= dateadd(day, 1, c.event_date)::timestamp_ntz)))
    or (c.product_history_fallback and (
      p.valid_from <> (select min(p0.valid_from) from {{ ref('dim_product') }} p0 where p0.product_id = p.product_id)
      or dateadd(day, 1, c.event_date)::timestamp_ntz > p.valid_from))
  )
)
select * from boundary_failures
union all select * from context_failures
{% endtest %}
