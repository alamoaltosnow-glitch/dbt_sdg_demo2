{{ config(materialized='table') }}

with hub_part as (
    select
        hub_part_hk,
        business_key,
        load_date
    from {{ ref('hub_part') }}
),

sat_part_current as (
    select
      t.*,
      true as flag_current
    --  load_date
    from 
      {{ ref('sat_part') }} t
    qualify row_number() over (partition by hub_part_hk order by load_date desc) = 1
),




sat_order_current as (
   select 
      t.*,
      true as flag_current
     --- load_date 
   from 
     {{ ref('sat_order') }} t
   qualify row_number() over (partition by hub_order_hk order by load_date desc) = 1
),

agg_part as (
    select
        l.hub_part_hk,
        count(distinct o.hub_order_hk) as total_orders,
        sum(o.o_totalprice)            as total_sales,
       -- sum(o.o_quantity)              as total_quantity,
        min(o.o_orderdate)             as first_order_date,
        max(o.o_orderdate)             as last_order_date,
        datediff(day,  max(o.o_orderdate), current_date) as days_since_last_order,
        datediff(year, max(o.o_orderdate), current_date) as years_since_last_order
    from 
       {{ ref('link_order_lineitem') }} l,
       sat_order_current o
    where 
       l.hub_order_hk = o.hub_order_hk  -- and
      -- l.hub_part_hk = o.hub_part_hk
    group by
       l.hub_part_hk
)

select

     h.business_key as bs_part_key,

     s.p_name,
     s.p_mfgr,
     s.p_brand,
     s.p_type,
     s.p_size,
     s.p_container,
     s.p_retailprice,
     s.p_comment,

    ---orders---------------------------
    a.total_orders,
    a.total_sales,
    a.first_order_date,
    a.last_order_date,
    a.days_since_last_order,
    a.years_since_last_order,

    h.hub_part_hk,
  --  s.sat_part_pk,
    s.flag_current,
    h.load_date       as load_date_hub,
    s.load_date       as load_date_sat,
    current_timestamp as load_date_bv
from 
  hub_part h
left join sat_part_current s
  on h.hub_part_hk = s.hub_part_hk
left join agg_part a
  on h.hub_part_hk = a.hub_part_hk  