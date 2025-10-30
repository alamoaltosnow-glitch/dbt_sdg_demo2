{{ config(materialized='table') }}

with hub as (
    select
        hub_customer_hk,
        business_key,
        load_date
    from {{ ref('hub_customer') }}
),

sat as (
    select
      *
     /* hub_customer_hk,
      sat_customer_pk,
      c_name,
      c_acctbal,
      c_mktsegment,
      c_address,
      c_nationkey,
      c_phone,
      c_comment,
      load_date*/
    from 
      {{ ref('snap_sat_customer') }}
    ---qualify row_number() over (partition by hub_customer_hk order by load_date desc) = 1
)

select
    h.business_key as c_custid,
    s.c_name,           
    s.c_acctbal,
    s.c_mktsegment,
    s.c_address,
    s.c_nationkey,
    s.c_phone,
    s.c_comment,

    CASE
        WHEN s.C_ACCTBAL < 0 THEN 'NEGATIVE'
        WHEN s.C_ACCTBAL = 0 THEN 'ZERO'
        WHEN s.C_ACCTBAL BETWEEN 1 AND 1000 THEN 'LOW'
        WHEN s.C_ACCTBAL BETWEEN 1001 AND 5000 THEN 'MEDIUM'
        ELSE 'HIGH'
    END AS id_dim_balance,

    CASE
        WHEN C_ACCTBAL < 0 THEN 'Negative Balance'
        WHEN C_ACCTBAL = 0 THEN 'Zero Balance'
        WHEN C_ACCTBAL BETWEEN 1 AND 1000  THEN 'Balance below 1000'
        WHEN C_ACCTBAL BETWEEN 1001 AND 5000 THEN 'Balance between 1000 and 5000'
        ELSE 'Balance over 5000'
    END AS desc_dim_balance,

    h.hub_customer_hk,
    s.sat_customer_pk,
   
    case when dbt_valid_to is null then true else false end as flag_current,

    s.dbt_valid_from                       as valid_from,
    coalesce(s.dbt_valid_to, '9999-12-31') as valid_to,
    h.load_date       as load_date_hub,
    s.load_date       as load_date_sat,
    current_timestamp as load_date_bv

from 
  hub h
left join sat s
  on h.hub_customer_hk = s.hub_customer_hk