{{ config(materialized='table') }}

with joined as (
    select
        --  Relaciones entre las entidades
        lnk_oc.link_order_customer_hk,
        lnk_oc.hub_order_hk,
        lnk_oc.hub_customer_hk,

        lnk_ol.link_order_lineitem_hk,

        --  Atributos del cliente
        sat_c.c_name,
        sat_c.c_mktsegment,
 
        --  Atributos de la línea
        sat_ol.l_linenumber,
        sat_ol.l_extendedprice,
        sat_ol.l_discount,
        sat_ol.l_extendedprice * (1 - sat_ol.l_discount) as net_sales,
        sat_ol.l_shipdate

    from {{ ref('link_order_customer') }} lnk_oc
    join {{ ref('sat_customer') }} sat_c
      on lnk_oc.hub_customer_hk = sat_c.hub_customer_hk

    join {{ ref('link_order_lineitem') }} lnk_ol
      on lnk_oc.hub_order_hk = lnk_ol.hub_order_hk

    join {{ ref('sat_lineitem') }} sat_ol
      on lnk_ol.link_order_lineitem_hk = sat_ol.link_order_lineitem_hk
)

select 
*
    /*hub_customer_hk,
    c_name,
    sum(net_sales) as total_sales,
    min(l_shipdate) as first_ship_date,
    max(l_shipdate) as last_ship_date
*/ from joined
--group by hub_customer_hk, c_name