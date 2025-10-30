{{ config(materialized='view') }}

select
    distinct
        o_orderstatus as order_status_key,
        case
            when o_orderstatus = 'O' then 'Open Order'
            when o_orderstatus = 'F' then 'Filled / Completed Order'
            when o_orderstatus = 'P' then 'Pending / In Process Order'
            else concat(o_orderstatus,' - Unknown Status')
        end as order_status_desc,
        current_timestamp() as load_date
from {{ ref('sat_order') }}
