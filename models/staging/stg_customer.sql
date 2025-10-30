{{ config(materialized='view') }}

with raw as (
    select * from {{ source('tpch_sf1', 'customer') }}
    ---SNOWFLAKE_SAMPLE_DATA_AG.TPCH_SF1.CUSTOMER
    --where c_custkey BETWEEN 99998 AND 100010 --100100
    --2610 where c_custkey in 
    --2610     (select o_custkey from SNOWFLAKE_SAMPLE_DATA_AG.TPCH_SF1.orders
     --2610    where o_orderkey between 4200001 and 4200034) 

)
select
  c_custkey,--::varchar as c_custkey,  
  c_name,             
  c_address,
  c_nationkey, --as c_nationkey,  
  c_phone,  
  c_acctbal,--::numeric as c_acctbal,  
  c_mktsegment,  
  c_comment
from raw