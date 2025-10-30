{{ config(materialized='table') }}

with hub_sup as (
    select
        hub_supplier_hk,
        business_key,
        load_date
    from {{ ref('hub_supplier') }}
),

sat_sup_current as (
    select
      hub_supplier_hk,
      sat_supplier_pk,
      s_name,
      s_address,
      s_nationkey,
      s_phone,
      s_acctbal,
      s_comment,
      true as flag_current,
      load_date
    from 
      {{ ref('sat_supplier') }} s--,
    qualify row_number() over (partition by s.hub_supplier_hk order by s.load_date desc) = 1
)

select
    h.business_key as bs_supplier_key,
    s.s_name,           
    s.s_address,
    s.s_nationkey,
    s.s_phone,
    s.s_acctbal,
    s.s_comment,


    ---accout balance--------------------
    CASE
        WHEN s.s_ACCTBAL < 0 THEN 'NEGATIVE'
        WHEN s.s_ACCTBAL = 0 THEN 'ZERO'
        WHEN s.s_ACCTBAL BETWEEN 1 AND 1000 THEN 'LOW'
        WHEN s.s_ACCTBAL BETWEEN 1001 AND 5000 THEN 'MEDIUM'
        ELSE 'HIGH'
    END AS id_dim_balance,

    CASE
        WHEN s_ACCTBAL < 0 THEN 'Negative Balance'
        WHEN s_ACCTBAL = 0 THEN 'Zero Balance'
        WHEN s_ACCTBAL BETWEEN 1 AND 1000  THEN 'Balance below 1000'
        WHEN s_ACCTBAL BETWEEN 1001 AND 5000 THEN 'Balance between 1000 and 5000'
        ELSE 'Balance over 5000'
    END AS desc_dim_balance,

    h.hub_supplier_hk,
    s.sat_supplier_pk,
    s.flag_current,
    h.load_date       as load_date_hub,
    s.load_date       as load_date_sat,
    current_timestamp as load_date_bv
from 
  hub_sup h
left join sat_sup_current s
  on h.hub_supplier_hk = s.hub_supplier_hk
