{{ config(materialized='view') }}

select
    distinct
        l_returnflag as line_returnflag_key,
        case
            when l_returnflag = 'A' then 'Accepted'
            when l_returnflag = 'R' then 'Returned'
            when l_returnflag = 'N' then 'Not returned'
            else concat(l_returnflag,' - Unknown Status')
        end as line_returnflag_desc,
        current_timestamp() as load_date
from {{ ref('sat_lineitem') }}
