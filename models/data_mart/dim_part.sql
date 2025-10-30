{{ config(materialized='table') }}

with buv as (
    select
        *
    from {{ ref('buv_part') }}
)

select

    {{ generate_sk_int(['BS_PART_KEY']) }} AS part_sk,
     --p_partkey,
     BS_PART_KEY,
     p_name,
     p_mfgr,
     p_brand,
     p_type,
     p_size,
     p_container,
     p_retailprice,
     p_comment,

    ---orders---------------------------
    total_orders,
    total_sales,
    first_order_date,
    last_order_date,
    days_since_last_order,
    years_since_last_order,

    -- hub_part_hk,
     flag_current,
     --load_date_hub,
     --load_date_sat,
     --load_date_bv,
     
     current_timestamp as load_date_dim
from 
  buv b
