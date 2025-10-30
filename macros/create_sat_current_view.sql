{% macro create_sat_current_view(sat_table, key_column, schema_name='RAW_VAULT', load_column='load_date') %}
{#
  Crea o reemplaza una vista *_current para una tabla SAT en Snowflake.
  Usa QUALIFY para quedarse con el último registro por hash key.
  --------------------------------------------------------------
  Parámetros:
    sat_table   → nombre del modelo SAT (ej: 'sat_order')
    key_column  → columna hash key (ej: 'hub_order_hk')
    schema_name → esquema donde crear la vista (default: STAGING_SAT_CURRENT)
    load_column → columna de orden (default: load_date)
#}

{% set database_name = target.database %}
{% set view_name = sat_table ~ '_current' %}
{% set full_view = database_name ~ '.' ~ schema_name ~ '.' ~ view_name %}

{% set sql %}
create or replace view {{ full_view }} as
select *
from {{ ref(sat_table) }}
qualify row_number() over (
  partition by {{ key_column }}
  order by {{ load_column }} desc
) = 1
{% endset %}

{% do log(' Creating view: ' ~ full_view, info=True) %}
{% do run_query(sql) %}
{% do log(' View created successfully: ' ~ full_view, info=True) %}
{% endmacro %}
