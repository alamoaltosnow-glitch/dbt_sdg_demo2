{{ config(materialized='table') }}

with hub_order as (
    select
        hub_order_hk,
        business_key,
        load_date
    from {{ ref('hub_order') }}
),

sat_order_current as (
   select 
       HUB_ORDER_HK,
       SAT_ORDER_PK,
       O_ORDERSTATUS,
       O_TOTALPRICE,
       O_ORDERDATE,
       O_ORDERPRIORITY,
       O_CLERK,
       O_SHIPPRIORITY,
       O_COMMENT,
       RECORD_SOURCE,
       RECORD_SOURCE_MD,
       RECORD_SOURCE_SC,
       RECORD_SOURCE_DB,
       true as flag_current,
       LOAD_DATE
   from 
     {{ ref('sat_order') }} o
   qualify row_number() over (partition by hub_order_hk order by load_date desc) = 1
)

select
       h.business_key as bs_order_key,
--HUB_ORDER_HK,
--SAT_ORDER_PK,
       O_ORDERSTATUS,
     O_TOTALPRICE,
O_ORDERDATE,
O_ORDERPRIORITY,
O_CLERK,
O_SHIPPRIORITY,
O_COMMENT,
RECORD_SOURCE,
RECORD_SOURCE_MD,
RECORD_SOURCE_SC,
RECORD_SOURCE_DB,
--true as flag_current,
--LOAD_DATE
    h.hub_order_hk,
    s.sat_order_pk,
    s.flag_current,
    h.load_date       as load_date_hub,
    s.load_date       as load_date_sat,
    current_timestamp as load_date_bv
from 
  hub_ORDER h
left join sat_order_current s
  on h.hub_order_hk = s.hub_order_hk
