{% macro generate_schema_name(custom_schema_name, node) -%}
  {%- if target.database | upper != 'SUPPLY_CHAIN_DEV' -%}
    {{ exceptions.raise_compiler_error('SupplySight transformations are pinned to SUPPLY_CHAIN_DEV.') }}
  {%- endif -%}
  {%- if target.warehouse | upper != 'SUPPLY_CHAIN_DEV_WH' -%}
    {{ exceptions.raise_compiler_error('SupplySight transformations are pinned to SUPPLY_CHAIN_DEV_WH.') }}
  {%- endif -%}
  {%- set layer = (custom_schema_name or target.schema) | upper -%}
  {%- if layer == 'DBT_TEST__AUDIT' -%}
    {%- set layer = 'INTERMEDIATE' -%}
  {%- endif -%}
  {%- if layer not in ['STAGING', 'INTERMEDIATE', 'CORE', 'MARTS'] -%}
    {{ exceptions.raise_compiler_error('Unsupported SupplySight dbt schema: ' ~ layer) }}
  {%- endif -%}
  {{ layer }}
{%- endmacro %}

{% macro generate_database_name(custom_database_name, node) -%}
  {%- if target.database | upper != 'SUPPLY_CHAIN_DEV' -%}
    {{ exceptions.raise_compiler_error('SupplySight transformations are pinned to SUPPLY_CHAIN_DEV.') }}
  {%- endif -%}
  SUPPLY_CHAIN_DEV
{%- endmacro %}
