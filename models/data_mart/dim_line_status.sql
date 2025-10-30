{{ config(materialized='table') }}

select
    line_status_key,
    line_status_desc
from {{ ref('buv_line_status') }}