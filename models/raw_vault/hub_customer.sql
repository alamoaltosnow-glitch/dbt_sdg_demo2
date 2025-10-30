{{ config(materialized='incremental', unique_key='hub_customer_hk' ) }}

with src as (  
  select * from {{ ref('stg_customer') }}
),
hub as (  
  select
    {{ dv_hash(['c_custkey']) }}          as hub_customer_hk,    
     c_custkey                            as business_key,    
     SUBSTR('{{ ref('stg_customer').name }}',5) as entity_name,
     '{{ ref('stg_customer') }}'          as record_source,
     '{{ ref('stg_customer').name }}'     as record_source_md,
     '{{ ref('stg_customer').schema }}'   as record_source_sc,
     '{{ ref('stg_customer').database }}' as record_source_db,
     current_timestamp() as load_date    
  from src 
  group by c_custkey)

select * from hub
{% if is_incremental() %}
where hub_customer_hk not in (select hub_customer_hk from {{ this }})
{% endif %}



