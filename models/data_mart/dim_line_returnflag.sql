{{ config(materialized='table') }}

select
    line_returnflag_key,
    line_returnflag_desc
from {{ ref('buv_line_returnflag') }}