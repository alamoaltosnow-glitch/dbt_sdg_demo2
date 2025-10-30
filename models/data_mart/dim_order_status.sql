{{ config(materialized='table') }}

select
    order_status_key,
    order_status_desc
from {{ ref('buv_order_status') }}