{{ config(materialized='incremental', unique_key='hub_part_hk' ) }}

with src as (  
  select * from {{ ref('stg_part') }}
),
hub as (  
  select
    {{ dv_hash(['p_partkey']) }}          as hub_part_hk,    
     p_partkey                            as business_key,    
     'PART'                           as entity_name,
     '{{ ref('stg_part') }}'          as record_source,
     '{{ ref('stg_part').name }}'     as record_source_md,
     '{{ ref('stg_part').schema }}'   as record_source_sc,
     '{{ ref('stg_part').database }}' as record_source_db,
     current_timestamp() as load_date    
  from src 
  group by p_partkey)

select * from hub
{% if is_incremental() %}
where hub_part_hk not in (select hub_part_hk from {{ this }})
{% endif %}



