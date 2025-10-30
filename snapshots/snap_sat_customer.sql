{% snapshot snap_sat_customer %}

{{ config(
    target_schema='raw_vault_snapshots',
    target_database='DWH_SDG_DEMO',
    unique_key='hub_customer_hk',
    strategy='check',
    check_cols=['sat_customer_pk']
) }}

select
    s.hub_customer_hk,
    s.sat_customer_pk,
    s.c_name,
    s.c_address,
    s.c_nationkey,
    s.c_phone,
    s.c_acctbal,  
    s.c_mktsegment,
    s.c_comment,
    s.load_date,
    s.record_source
from    {{ source('snapshots','sat_customer') }}  s
qualify row_number() over (
    partition by s.hub_customer_hk
    order by s.load_date desc
  ) = 1
{% endsnapshot %}

