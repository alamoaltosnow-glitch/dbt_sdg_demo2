{{ config(materialized='view') }}

with raw as (
    select * from {{ source('tpch_sf1', 'nation') }}
)
select
  n_nationkey,
  n_name,
  n_regionkey,
  n_comment
from raw