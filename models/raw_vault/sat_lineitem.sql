{{ config(materialized='incremental', unique_key='sat_lineitem_pk') }}

with src as (
  select * from {{ ref('stg_lineitem') }}
),
sat as (
  select 

      {{ dv_hash(['l_orderkey','l_linenumber','l_partkey','l_suppkey']) }} as link_order_lineitem_hk,

      --hashdiff
      {{ dv_hash([
        'l_linenumber',
        'l_quantity',
        'l_extendedprice',
        'l_discount',
        'l_tax',
        'l_returnflag',
        'l_linestatus',
        'l_shipdate',
        'l_commitdate',
        'l_receiptdate',
        'l_shipinstruct',
        'l_shipmode',
        'l_comment' ]) }} as sat_lineitem_pk,

       /* l_orderkey,
        l_partkey,
        l_suppkey,*/
        l_linenumber,
        l_quantity,
        l_extendedprice,
        l_discount,
        l_tax,
        l_returnflag,
        l_linestatus,
        l_shipdate,
        l_commitdate,
        l_receiptdate,
        l_shipinstruct,
        l_shipmode,
        l_comment,
       
       '{{ ref('stg_orders') }}'          as record_source,
       '{{ ref('stg_orders').name }}'     as record_source_md,
       '{{ ref('stg_orders').schema }}'   as record_source_sc,
       '{{ ref('stg_orders').database }}' as record_source_db,
       current_timestamp() as load_date 
 from src)
      
select * from sat {% if is_incremental() %} where sat_lineitem_pk not in (select sat_lineitem_pk from {{ this }})
{% endif %} 
