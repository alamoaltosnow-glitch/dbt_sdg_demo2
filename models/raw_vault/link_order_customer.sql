{{ config(materialized='incremental', unique_key='link_order_customer_hk') }}
with src as (  
    select o_orderkey, o_custkey from {{ ref('stg_orders') }}
),
link as (  
  select   
    {{ dv_hash(['o_orderkey','o_custkey']) }} as link_order_customer_hk, 
    {{ dv_hash(['o_orderkey']) }} as hub_order_hk,
    {{ dv_hash(['o_custkey']) }}  as hub_customer_hk,
    
    '{{ ref('stg_orders') }}' as record_source,
    current_timestamp()       as load_date    
    
  from src  
  group by o_orderkey, o_custkey)

select
  * 
from 
  link{% if is_incremental() %}
where 
  link_order_customer_hk not in (select link_order_customer_hk from {{ this }}){% endif %}