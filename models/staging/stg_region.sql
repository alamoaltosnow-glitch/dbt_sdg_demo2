{{ config(materialized='view') }}

with raw as (
    select * from {{ source('tpch_sf1', 'region') }}
)
select
  r_regionkey,
  r_name,
  r_comment
from raw