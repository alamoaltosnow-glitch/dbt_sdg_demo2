{{ config(materialized='view') }}

with raw as (
    select * from {{ source('tpch_sf1', 'part') }}
  --2610  where p_partkey in 
  --2610       (select l_partkey from SNOWFLAKE_SAMPLE_DATA_AG.TPCH_SF1.lineitem
  --2610        where l_orderkey between 4200001 and 4200034) 
)
select
  p_partkey,
  p_name,
  p_mfgr,
  p_brand,
  p_type,
  p_size,
  p_container,
  p_retailprice,
  p_comment
from raw