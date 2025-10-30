{{ config(materialized='view') }}

with raw as (
    select * from {{ source('tpch_sf1', 'orders') }}
  --2610  where o_orderkey between 4200001 and 4200034
)

select
    o_orderkey::varchar as o_orderkey,
    o_custkey::varchar as o_custkey,
    o_orderstatus as o_orderstatus,  
    -- o_totalprice::numeric as o_totalprice,
    o_totalprice as o_totalprice,
    o_orderdate::date as o_orderdate,
    o_orderpriority as o_orderpriority,  
    o_clerk as o_clerk,
    o_shippriority as o_shippriority, 
    o_comment as o_comment
from raw


