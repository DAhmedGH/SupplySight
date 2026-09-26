{% test unique_combination_of_columns(model, combination_of_columns) %}
select {{ combination_of_columns | join(', ') }}, count(*) as row_count
from {{ model }}
group by {{ combination_of_columns | join(', ') }}
having count(*) > 1
{% endtest %}

{% test no_unexpected_nulls(model, required_columns) %}
select * from {{ model }}
where {% for column in required_columns %}{{ column }} is null{% if not loop.last %} or {% endif %}{% endfor %}
{% endtest %}

{% test source_stage_reconciliation(model, source_name, source_table, key_columns) %}
{% set source_relation = source(source_name, source_table) %}
{% set raw_keys = [] %}
{% set stage_keys = [] %}
{% set join_conditions = [] %}
{% set reverse_conditions = [] %}
{% for key in key_columns %}
  {% do raw_keys.append("nullif(trim(to_varchar(" ~ key ~ ")), '') as k" ~ loop.index) %}
  {% do stage_keys.append("nullif(trim(to_varchar(" ~ key | lower ~ ")), '') as k" ~ loop.index) %}
  {% do join_conditions.append('r.k' ~ loop.index ~ ' = s.k' ~ loop.index) %}
  {% do reverse_conditions.append('s.k' ~ loop.index ~ ' = r.k' ~ loop.index) %}
{% endfor %}
with raw_keys as (
    select {{ raw_keys | join(', ') }},
           upper(trim(_quality_status)) as quality_status,
           to_json(array_sort(coalesce(_quality_issues, array_construct()))) as quality_issues,
           coalesce(upper(trim(_quality_status)) = 'QUARANTINED', false) as is_quarantined
    from {{ source_relation }}
), stage_keys as (
    select {{ stage_keys | join(', ') }},
           upper(trim(quality_status)) as quality_status,
           to_json(array_sort(coalesce(quality_issues, array_construct()))) as quality_issues,
           coalesce(is_quarantined, false) as is_quarantined
    from {{ model }}
), raw_duplicates as (
    select {{ raw_keys | join(', ') }}, count(*) as n from {{ source_relation }}
    group by {{ key_columns | join(', ') }} having count(*) > 1
), counts as (
    select (select count(*) from raw_keys) as raw_count,
           (select count(*) from stage_keys) as stage_count,
           (select count(*) from raw_duplicates) as duplicate_key_count
), missing_from_stage as (
    select r.* from raw_keys r left join stage_keys s on {{ join_conditions | join(' and ') }}
    where {% for key in key_columns %}s.k{{ loop.index }} is null{% if not loop.last %} or {% endif %}{% endfor %}
), missing_from_raw as (
    select s.* from stage_keys s left join raw_keys r on {{ reverse_conditions | join(' and ') }}
    where {% for key in key_columns %}r.k{{ loop.index }} is null{% if not loop.last %} or {% endif %}{% endfor %}
), quality_mismatches as (
    select r.* from raw_keys r inner join stage_keys s on {{ join_conditions | join(' and ') }}
    where r.quality_status is distinct from s.quality_status
       or r.quality_issues is distinct from s.quality_issues
       or r.is_quarantined is distinct from s.is_quarantined
)
select 'row_count_or_raw_duplicate_mismatch' as failure
from counts where raw_count <> stage_count or duplicate_key_count > 0
union all
select 'raw_key_missing_from_stage' from missing_from_stage
union all
select 'stage_key_missing_from_raw' from missing_from_raw
union all
select 'quality_metadata_mismatch' from quality_mismatches
{% endtest %}
