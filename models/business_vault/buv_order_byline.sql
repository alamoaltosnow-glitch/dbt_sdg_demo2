{{ config(materialized='table') }}

with 
sat_order_current as (
    select *
    from {{ ref('sat_order') }}
    qualify row_number() over (
        partition by hub_order_hk
        order by load_date desc
    ) = 1
),
sat_lineitem_current as (
    select *
    from {{ ref('sat_lineitem') }}
    qualify row_number() over (
        partition by link_order_lineitem_hk
        order by load_date desc
    ) = 1
),
sat_customer_current as (
    select *
    from {{ ref('sat_customer') }}
    qualify row_number() over (
        partition by hub_customer_hk
        order by load_date desc
    ) = 1
),
sat_supplier_current as (
    select *
    from {{ ref('sat_supplier') }}
    qualify row_number() over (
        partition by hub_supplier_hk
        order by load_date desc
    ) = 1
),
sat_part_current as (
    select *
    from {{ ref('sat_part') }}
    qualify row_number() over (
        partition by hub_part_hk
        order by load_date desc
    ) = 1
),
sat_part_supplier_current as (
    select *
    from {{ ref('sat_part_supplier') }}
    qualify row_number() over (
        partition by link_part_supplier_hk
        order by load_date desc
    ) = 1
),
base as (
    select
       -- Hash keys
        lnk_ol.link_order_lineitem_hk,
        lnk_ol.hub_order_hk,
        lnk_ol.hub_part_hk,
        lnk_ol.hub_supplier_hk,
        lnk_oc.hub_customer_hk,

        --Claves business - origen
        hub_o.business_key  as BS_ORDER_KEY,
        sat_li.l_linenumber as BS_LINENUMBER_KEY,
        hub_p.business_key  as BS_PART_KEY,
        hub_s.business_key  as BS_SUPPLIER_KEY,
        hub_c.business_key  as BS_CUSTOMER_KEY,

        -- Fechas y status
        sat_o.o_orderdate,
        --sat_o.o_orderstatus,
        sat_li.l_shipdate,
        sat_li.l_commitdate,
        sat_li.l_receiptdate,
        
        sat_li.l_returnflag,
        sat_li.l_linestatus,

        -- Métricas económicas por orden
        sat_o.o_totalprice, --as unit_price,

        -- Métricas económicas por línea
        sat_li.l_extendedprice,
        sat_li.l_quantity      as quantity,
        CAST(sat_ps.ps_supplycost AS NUMBER(18, 2))  as unit_cost,
        CAST(sat_p.p_retailprice  AS NUMBER(18, 2))  as unit_price,
        --sat_li.l_extendedprice as total_gross_sales,
        CAST((unit_price * quantity) AS NUMBER(18, 2))  as total_gross_sales,
        CAST(sat_li.l_discount AS NUMBER(18, 2))        as discount_pct ,
        CAST((total_gross_sales * discount_pct) AS NUMBER(18, 2)) as discount_amount,
        CAST((total_gross_sales - discount_amount) AS NUMBER(18, 2)) as total_net_sales,
        CAST(sat_li.l_tax AS NUMBER(18, 2))                as tax_pct,
        CAST((total_net_sales * tax_pct)  AS NUMBER(18, 2)) as tax_amount,
        (total_net_sales + tax_amount)                as total_invoice_amount,
        CAST((unit_cost * quantity) AS NUMBER(18, 2)) as total_cost,
        total_net_sales - total_cost                  as total_gross_profit,
        total_gross_profit / total_net_sales          as total_gross_profit_margin_pct,
        case when discount_pct <> 0 then true else false end flag_with_discount ,
        case when tax_pct <> 0 then true else false end flag_with_tax ,
        ---  case when sat_li.l_extendedprice = total_gross_sales then true else false end as check_gross_sales,


        -- Comentarios y descripción
        sat_li.l_comment,
        sat_c.c_name as customer_name,
        sat_s.s_name as supplier_name,
        sat_p.p_name as part_name,
        sat_p.p_brand,
        sat_p.p_type,
        sat_p.p_container
    from 

         {{ ref('link_order_lineitem') }} lnk_ol

    ----hubs    
    join {{ ref('hub_order') }}  hub_o --DWH_SDG_DEMO.RAW_VAULT.hub_order 
     on lnk_ol.hub_order_hk = hub_o.hub_order_hk

    join {{ ref('hub_part') }} hub_p --DWH_SDG_DEMO.RAW_VAULT.hub_part 
     on lnk_ol.hub_part_hk = hub_p.hub_part_hk  

    join {{ ref('hub_supplier') }} hub_s --DWH_SDG_DEMO.RAW_VAULT.hub_supplier
     on lnk_ol.hub_supplier_hk = hub_s.hub_supplier_hk 

    join sat_order_current sat_o
        on hub_o.hub_order_hk = sat_o.hub_order_hk

    join sat_part_current sat_p
        on hub_p.hub_part_hk = sat_p.hub_part_hk 

    join sat_supplier_current sat_s
        on hub_s.hub_supplier_hk = sat_s.hub_supplier_hk 

    join sat_lineitem_current sat_li
       on lnk_ol.link_order_lineitem_hk = sat_li.link_order_lineitem_hk
      
    join {{ ref('link_part_supplier') }}  lnk_ps ----DWH_SDG_DEMO.RAW_VAULT.link_part_supplier
       on  ---lnk_ps.hub_order_hk = hub_o.hub_order_hk and
         lnk_ps.hub_part_hk = hub_p.hub_part_hk and
         lnk_ps.hub_supplier_hk = hub_s.hub_supplier_hk   

   join sat_part_supplier_current sat_ps 
      on sat_ps.LINK_PART_SUPPLIER_HK = lnk_ps.LINK_PART_SUPPLIER_HK 
            
    join {{ ref('link_order_customer') }} lnk_oc  ---DWH_SDG_DEMO.RAW_VAULT.link_order_customer
        on hub_o.hub_order_hk = lnk_oc.hub_order_hk

    join {{ ref('hub_customer') }} hub_c --DWH_SDG_DEMO.RAW_VAULT.hub_customer
        on hub_c.hub_customer_hk = lnk_oc.hub_customer_hk
        
    join sat_customer_current sat_c
        on hub_c.hub_customer_hk = sat_c.hub_customer_hk 
    
   ---duda      
   /* join sat_order_current sat_o
        on lnk_ol.hub_order_hk = sat_o.hub_order_hk
    join sat_part_current sat_p
        on lnk_ol.hub_part_hk = sat_p.hub_part_hk 
    join sat_supplier_current sat_s
        on lnk_ol.hub_supplier_hk = sat_s.hub_supplier_hk 
    */ 
    )

select * from base ---where order = 4200002 --- and LINENUMBER =1
