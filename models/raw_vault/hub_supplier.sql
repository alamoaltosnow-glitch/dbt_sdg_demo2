{{ config(materialized='incremental', unique_key='hub_supplier_hk' ) }}

with src as (  
  select * from {{ ref('stg_supplier') }}
),
hub as (  
  select
    {{ dv_hash(['s_suppkey']) }}          as hub_supplier_hk,    
     s_suppkey                            as business_key,    
     'SUPPLIER'                           as entity_name,
     '{{ ref('stg_supplier') }}'          as record_source,
     '{{ ref('stg_supplier').name }}'     as record_source_md,
     '{{ ref('stg_supplier').schema }}'   as record_source_sc,
     '{{ ref('stg_supplier').database }}' as record_source_db,
     current_timestamp() as load_date    
  from src 
  group by s_suppkey)

select * from hub
{% if is_incremental() %}
where hub_supplier_hk not in (select hub_supplier_hk from {{ this }})
{% endif %}



