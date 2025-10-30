{{ config(materialized='view') }}

select
    distinct
        l_linestatus as line_status_key,
        case
            when l_linestatus = 'O' then 'Open'
            when l_linestatus = 'F' then 'Filled / Completed'
            when l_linestatus = 'P' then 'Pending / In Process'
            else concat(l_linestatus,' - Unknown Status')
        end as line_status_desc,
        current_timestamp() as load_date
from {{ ref('sat_lineitem') }}
