{{ config(materialized='table') }}

with buv as (
    select
        *
    from {{ ref('buv_order') }}
)

select
   {{ generate_sk_int(['BS_ORDER_KEY']) }} AS ORDER_sk,
   -- O_ORDERKEY, 
    BS_ORDER_KEY,




O_ORDERSTATUS
,O_TOTALPRICE
,O_ORDERDATE
,O_ORDERPRIORITY
,O_CLERK
,O_SHIPPRIORITY
 O_COMMENT,
/*RECORD_SOURCE
RECORD_SOURCE_MD
RECORD_SOURCE_SC
RECORD_SOURCE_DB
HUB_ORDER_HK
SAT_ORDER_PK
FLAG_CURRENT
LOAD_DATE_HUB
LOAD_DATE_SAT
LOAD_DATE_BV
*/



    flag_current,   
    current_timestamp as load_date_dim
     --hub_ORDER_hk,
     --sat_ORDER_pk,     
     --load_date_hub,
     --load_date_sat,
     --load_date_bv,
from 
  buv b
