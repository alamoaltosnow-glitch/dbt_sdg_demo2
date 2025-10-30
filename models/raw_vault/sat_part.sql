{{ config(materialized='incremental', unique_key='sat_part_pk') }}

with src as (
  select * from {{ ref('stg_part') }}
),
sat as (
  select 
  
      {{ dv_hash(['p_partkey']) }} as hub_part_hk,

      --hashdiff
      {{ dv_hash(['p_name',
                  'p_mfgr',
                  'p_brand',
                  'p_type',
                  'p_size',
                  'p_container',
                  'p_retailprice',
                  'p_comment' ]) }} as sat_part_pk,

     p_name,
     p_mfgr,
     p_brand,
     p_type,
     p_size,
     p_container,
     p_retailprice,
     p_comment,

     '{{ ref('stg_part') }}'          as record_source,
     '{{ ref('stg_part').name }}'     as record_source_md,
     '{{ ref('stg_part').schema }}'   as record_source_sc,
     '{{ ref('stg_part').database }}' as record_source_db,
     current_timestamp() as load_date 
 from src)
      
select * from sat {% if is_incremental() %} where sat_part_pk not in (select sat_part_pk from {{ this }})
{% endif %} 



