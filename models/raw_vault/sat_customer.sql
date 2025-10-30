{{ config(materialized='incremental', unique_key='sat_customer_pk') }}

with src as (
  select * from {{ ref('stg_customer') }}
),
sat as (
  select 
  
      {{ dv_hash(['c_custkey']) }} as hub_customer_hk,

      --hashdiff
      {{ dv_hash(['c_name','c_address','c_nationkey','c_phone','c_acctbal','c_mktsegment','c_comment']) }} as sat_customer_pk,

      c_name,    
      c_address,
      c_nationkey,    
      c_phone,    
      c_acctbal,    
      c_mktsegment,    
      c_comment, 

     '{{ ref('stg_customer') }}'          as record_source,
     '{{ ref('stg_customer').name }}'     as record_source_md,
     '{{ ref('stg_customer').schema }}'   as record_source_sc,
     '{{ ref('stg_customer').database }}' as record_source_db,
     current_timestamp() as load_date 
 from src)
      
select * from sat {% if is_incremental() %} where sat_customer_pk not in (select sat_customer_pk from {{ this }})
{% endif %} 



