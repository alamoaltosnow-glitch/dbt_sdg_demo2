{{ config(materialized='table') }}

with buv as (
    select
        *
    from {{ ref('buv_customer') }}
)

select
   {{ generate_sk_int(['BS_CUSTOMER_KEY']) }} AS customer_sk,
    --c_custkey, 
    BS_CUSTOMER_KEY,
    c_name,           
    c_acctbal,
    c_mktsegment,
    c_address,
    c_nationkey,
    c_phone,
    c_comment,

    ---orders---------------------------
    total_orders,
    total_sales,
    first_order_date,
    last_order_date,
    days_since_last_order,
    years_since_last_order,

    ---accout balance--------------------
     id_dim_balance,
     desc_dim_balance,
     flag_current,
     current_timestamp as load_date_dim
     --hub_customer_hk,
     --sat_customer_pk,
     --load_date_hub,
     --load_date_sat,
     --load_date_bv,
     
from 
  buv b
