{{ config(materialized='table') }}

with dates as (

    -- Generate roughly 6 years of dates starting from 2019-01-01 (covers 2019-2023)
    select
        dateadd(
            day,
            row_number() over (order by 0) - 1,
            to_date('2019-01-01')
        ) as full_date
    from table(generator(rowcount => 365 * 6))

)

select
    -- Keep the date_key rule consistent with stg_index_daily
    to_number(to_char(full_date, 'YYYYMMDD')) as date_key,
    full_date,

    extract(year   from full_date) as year,
    extract(month  from full_date) as month,
    extract(day    from full_date) as day,
    extract(quarter from full_date) as quarter,
    to_char(full_date, 'DY')       as weekday_short,
    to_char(full_date, 'DAY')      as weekday_full

from dates
