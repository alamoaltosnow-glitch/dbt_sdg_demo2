{{ config(materialized='incremental', unique_key='link_order_lineitem_hk') }}

with src as (  
    select l_orderkey, l_linenumber, l_partkey, l_suppkey from {{ ref('stg_lineitem') }}
),

link as (  
  select   
    {{ dv_hash(['l_orderkey','l_linenumber','l_partkey','l_suppkey']) }} as link_order_lineitem_hk,
    {{ dv_hash(['l_orderkey']) }} as hub_order_hk,
    {{ dv_hash(['l_partkey'])  }} as hub_part_hk,
    {{ dv_hash(['l_suppkey'])  }} as hub_supplier_hk,

    '{{ ref('stg_lineitem') }}'          as record_source,
    current_timestamp() as load_date    
    
  from src
    group by l_orderkey, l_linenumber, l_partkey, l_suppkey)
  
 
select
  * 
from 
  link{% if is_incremental() %}
where 
  link_order_lineitem_hk not in (select link_order_lineitem_hk from {{ this }}){% endif %}
