{{ config(materialized='table') }}

select
-- LINK_ORDER_LINEITEM_HK
--,HUB_ORDER_HK
--,bv_base.HUB_PART_HK
--,HUB_SUPPLIER_HK
--,HUB_CUSTOMER_HK

--,ORDER_KEY
----,LINENUMBER_KEY
--,PART_KEY
--SUPPLIER_KEY
--,CUSTOMER_KEY

{{ generate_sk_int(['bv_base.bs_order_key', 'bv_base.bs_part_key', 'BS_LINENUMBER_KEY']) }} AS order_line_sK

--surrogate_keys
 ,BS_LINENUMBER_KEY AS LINENUMBER_KEY
 ,dim_order.order_sk
 ,dim_part.part_sk
 ,dim_sup.supplier_sk
 ,dim_cust.customer_sk

---dates
,dim_cal_1.DATE_SK AS ORDERDATE_SK
,dim_cal_2.DATE_SK AS LINESHIPDATE_SK
,dim_cal_3.DATE_SK AS LINECOMMITDATE_SK
,dim_cal_4.DATE_SK AS LINERECEIPTDATE_SK

,dim_l_st.line_status_key
,dim_l_rf.line_returnflag_key

---,bv_base.O_ORDERSTATUS as order_status_key
----,L_LINESTATUS as line_status_key
--,L_RETURNFLAG as line_returnflag_key

,QUANTITY
,UNIT_COST
,UNIT_PRICE
,TOTAL_GROSS_SALES
,DISCOUNT_PCT
,DISCOUNT_AMOUNT
,TOTAL_NET_SALES
,TAX_PCT
,TAX_AMOUNT
,TOTAL_INVOICE_AMOUNT
,TOTAL_COST
,TOTAL_GROSS_PROFIT
,TOTAL_GROSS_PROFIT_MARGIN_PCT
,FLAG_WITH_DISCOUNT
,FLAG_WITH_TAX

from {{ ref('buv_order_byline') }} bv_base

  join  {{ ref('dim_calendar') }}  dim_cal_1
  on bv_base.o_orderdate = dim_cal_1.date_ymd

  join  {{ ref('dim_calendar') }}  dim_cal_2
  on bv_base.L_SHIPDATE = dim_cal_2.date_ymd

  join  {{ ref('dim_calendar') }}  dim_cal_3
  on bv_base.L_COMMITDATE = dim_cal_3.date_ymd

  join  {{ ref('dim_calendar') }}  dim_cal_4
  on bv_base.L_RECEIPTDATE = dim_cal_4.date_ymd

  LEFT JOIN {{ ref('dim_order') }} dim_order
  ON bv_base.bs_order_key = dim_order.bs_order_key

  LEFT JOIN {{ ref('dim_part') }} dim_part
  ON bv_base.bs_part_key = dim_part.bs_part_key

  LEFT JOIN {{ ref('dim_customer') }} dim_cust
  ON bv_base.bs_customer_key = dim_cust.bs_customer_key

  LEFT JOIN {{ ref('dim_supplier') }} dim_sup
  ON bv_base.bs_supplier_key = dim_sup.bs_supplier_key

  LEFT JOIN {{ ref('dim_line_status') }} dim_l_st
  ON bv_base.L_LINESTATUS = dim_l_st.line_status_key

 LEFT JOIN {{ ref('dim_line_returnflag') }} dim_l_rf
  ON bv_base.L_RETURNFLAG = dim_l_rf.line_returnflag_key
