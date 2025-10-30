{{ config(materialized='incremental', unique_key='sat_order_pk') }}

with src as (
  select * from {{ ref('stg_orders') }}
),
sat as (
  select 

      {{ dv_hash(['o_orderkey']) }} as hub_order_hk,

      --hashdiff
      {{ dv_hash(['o_orderstatus',
                  'o_totalprice',
                  'o_orderdate',
                  'o_orderpriority',
                  'o_clerk',
                  'o_shippriority',
                  'o_comment']) }} as sat_order_pk,

      o_orderstatus,
      o_totalprice,
      o_orderdate,
      o_orderpriority,
      o_clerk,
      o_shippriority,
      o_comment, 

     '{{ ref('stg_orders') }}'          as record_source,
     '{{ ref('stg_orders').name }}'     as record_source_md,
     '{{ ref('stg_orders').schema }}'   as record_source_sc,
     '{{ ref('stg_orders').database }}' as record_source_db,
     current_timestamp() as load_date 
 from src)
      
select * from sat {% if is_incremental() %} where sat_order_pk not in (select sat_order_pk from {{ this }})
{% endif %} 
