{{ config(materialized='incremental', unique_key='link_part_supplier_hk') }}

with src as (  
    select ps_partkey, ps_suppkey from {{ ref('stg_partsupp') }}
),

link as (  
  select   
  
    {{ dv_hash(['ps_partkey','ps_suppkey']) }} as link_part_supplier_hk,    
    {{ dv_hash(['ps_partkey']) }} as hub_part_hk,
    {{ dv_hash(['ps_suppkey']) }} as hub_supplier_hk,
    
    '{{ ref('stg_partsupp') }}' as record_source,
    current_timestamp()         as load_date    
    
  from src ) 

select
  * 
from 
  link{% if is_incremental() %}
where 
  link_part_supplier_hk not in (select link_part_supplier_hk from {{ this }}){% endif %}