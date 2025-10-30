{{ config(materialized='incremental', unique_key='sat_part_supplier_pk') }}

with src as (
  select * from {{ ref('stg_partsupp') }}
),
sat as (
  select 
  
      {{ dv_hash(['ps_partkey','ps_suppkey']) }} as link_part_supplier_hk,

      --hashdiff
      {{ dv_hash(['ps_suppkey',
                  'ps_availqty',
                  'ps_supplycost',
                  'ps_comment' ]) }} as sat_part_supplier_pk,

       ps_availqty,
       ps_supplycost,
       ps_comment,

     '{{ ref('stg_part') }}'          as record_source,
     '{{ ref('stg_part').name }}'     as record_source_md,
     '{{ ref('stg_part').schema }}'   as record_source_sc,
     '{{ ref('stg_part').database }}' as record_source_db,
     current_timestamp() as load_date 
 from src)
      
select * from sat {% if is_incremental() %} where sat_part_supplier_pk not in (select sat_part_supplier_pk from {{ this }})
{% endif %} 



