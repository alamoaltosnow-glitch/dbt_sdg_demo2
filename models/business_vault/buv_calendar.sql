{{ config(materialized='incremental', unique_key='date_key' ) }}

-- ==========================================================
-- DIM_TIME
-- Time dimension generated dynamically from 
-- Author: Alejandra
-- ==========================================================

with date_range as (

    -- Get min and max order dates from TPC-H, but including the current date
    select
        min(o_orderdate)                 as min_date_ord_old,
        '1991-01-01'                     as min_date_ord,
        dateadd(year, -1, min_date_ord)  as min_date,
        max(o_orderdate)                 as max_date_ord,
        current_date                     as max_date
   --- from llave llave palabra_source('stg_orders_date', 'stg_orders') llave llave
   from {{ ref('sat_order') }}
),

dates as (

    -- Generate one row per day between min and max
    select
      dateadd(day, seq4(), min_date) as full_date
    from date_range,
      table(generator(rowcount => 15000))
),

final as (

    select
      to_number(to_char(full_date, 'YYYYMMDD')) as date_key,
      cast(full_date as date) as date_ymd,
      full_date,
      to_char(full_date, 'YYYY-MM-DD') as date_str,
      to_char(full_date, 'YYYY') as year,
      to_char(full_date, 'MM') as month,
      to_char(full_date, 'Mon') as month_abbr,
      trim(to_char(full_date, 'MMMM')) as month_name,

      dayofweekiso(full_date) as day_of_week,

      day(full_date)             as day_of_month_number,
      TO_CHAR(full_date, 'DD')   as day_of_month_w_zeros,
      dayofyear(full_date)       as day_of_year,
      weekiso(full_date)         as week_of_year,
      month(full_date)           as month_number,
      year(full_date)            as year_number,

      quarter(full_date)              as quarter_number,
      concat('Q', quarter(full_date)) as quarter_name,
   
      to_char(full_date, 'DY')   as day_abbr,
      -- to_char(full_date, 'DYDY') as day_name,

      date_trunc('month', full_date) as first_day_of_month,
      last_day(full_date) as last_day_of_month,
      date_trunc('quarter', full_date) as first_day_of_quarter,
      dateadd(day, -1, dateadd(quarter, 1, date_trunc('quarter', full_date))) as last_day_of_quarter,
      date_trunc('year', full_date) as first_day_of_year,
      dateadd(day, -1, dateadd(year, 1, date_trunc('year', full_date))) as last_day_of_year,

      -- Flags
      case when dayofweekiso(full_date) in (6,7) then true else false end as is_weekend,
      case when dayofweekiso(full_date) in (6)   then true else false end as is_saturday,
      case when dayofweekiso(full_date) in (7)   then true else false end as is_sunday,
      false as is_holiday,  -- you can enrich later with a holidays table

      -- Combinations
      to_char(full_date, 'YYYY-MM') as year_month,
      concat(YEAR,'-Q',quarter(full_date)) as year_quarter,

     --EXTRA
     TO_CHAR(full_date, 'YYYY') AS year_four_digits,
     TO_CHAR(full_date, 'YY') AS year_last_two_digits,

     current_timestamp() as created_at

from dates

)

select * from final
order by full_date
/*
select 
    min(full_date) as min_date,
    max(full_date) as max_date, current_date  from final */

--select * from final where full_date between '2025-01-01' and current_date