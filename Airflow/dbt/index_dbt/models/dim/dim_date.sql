{{ config(materialized='table') }}

with dates as (

    -- 从 2019-01-01 开始生成大约 6 年的日期（足够覆盖 2019-2023）
    select
        dateadd(
            day,
            row_number() over (order by 0) - 1,
            to_date('2019-01-01')
        ) as full_date
    from table(generator(rowcount => 365 * 6))

)

select
    -- 和 stg_index_daily 里的 date_key 规则保持一致
    to_number(to_char(full_date, 'YYYYMMDD')) as date_key,
    full_date,

    extract(year   from full_date) as year,
    extract(month  from full_date) as month,
    extract(day    from full_date) as day,
    extract(quarter from full_date) as quarter,
    to_char(full_date, 'DY')       as weekday_short,
    to_char(full_date, 'DAY')      as weekday_full

from dates