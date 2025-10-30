{{ config(materialized='incremental', unique_key='sat_supplier_pk') }}

with src as (
  select * from {{ ref('stg_supplier') }}
),
sat as (
  select 
  
      {{ dv_hash(['s_suppkey']) }} as hub_supplier_hk,

      --hashdiff
      {{ dv_hash(['s_name','s_address','s_nationkey','s_phone','s_acctbal','s_comment']) }} as sat_supplier_pk,

      s_name,    
      s_address,
      s_nationkey,    
      s_phone,    
      s_acctbal,    
      s_comment, 

     '{{ ref('stg_supplier') }}'          as record_source,
     '{{ ref('stg_supplier').name }}'     as record_source_md,
     '{{ ref('stg_supplier').schema }}'   as record_source_sc,
     '{{ ref('stg_supplier').database }}' as record_source_db,
     current_timestamp() as load_date 
 from src)
      
select * from sat {% if is_incremental() %} where sat_supplier_pk not in (select sat_supplier_pk from {{ this }})
{% endif %} 



