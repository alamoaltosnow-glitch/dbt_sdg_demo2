{{ config(materialized='table') }}

with buv as (
    select
        *
    from {{ ref('buv_supplier') }}
)

select
   {{ generate_sk_int(['BS_SUPPLIER_KEY']) }} AS supplier_sk,
    --s_custkey, 
    BS_SUPPLIER_KEY,
    s_name,           
    s_address,
    s_nationkey,
    s_phone,
    s_acctbal,
    s_comment,

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
