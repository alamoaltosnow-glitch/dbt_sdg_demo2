{{ config(materialized='view') }}

with raw as (
    select * from {{ source('tpch_sf1', 'lineitem') }}
   --2610 where l_orderkey between 4200001 and 4200034
)

select  
    l_orderkey, 
    l_linenumber, 
    l_partkey,--::varchar as l_partkey,  
    l_suppkey,--::varchar as l_suppkey,  
    l_quantity,--::numeric as l_quantity,  
    l_extendedprice,--::numeric as l_extendedprice,  
    l_discount,--::numeric as l_discount,  
    l_tax,--::numeric as l_tax,  
    l_returnflag,-- as l_returnflag,  
    l_linestatus,-- as l_linestatus,  
    l_shipdate,--::date as l_shipdate,  
    l_commitdate,--::date as l_commitdate,  
    l_receiptdate,--::date as l_receiptdate,  
    l_shipinstruct,-- as l_shipinstruct,  
    l_shipmode,-- as l_shipmode,  
    l_comment ---as l_comment
from raw

