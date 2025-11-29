{{ config(materialized='table') }}

with raw as (

    select
        -- date 列是 20190102 这种整数，先转成字符串再转 DATE
        to_date(to_char("DATE"), 'YYYYMMDD') as trade_date,

        -- table_name 是指数ID（1000001等），统一转成字符串 index_code
        cast(table_name as varchar) as index_code,

        -- 映射价格和成交量相关字段
        cast(o as float) as open,
        cast(h as float) as high,
        cast(l as float) as low,
        cast(c as float) as close,
        cast(v as float) as volume,
        cast(a as float) as amount

    from {{ source('index_raw', 'INDEX_DAILY_2019_2023_FILTERED') }}

    -- 如果你想只保留部分指数，可以在这里加 where table_name in (...)
    -- where table_name in (1000001, 2399001, 2399006, ...)

)

select
    trade_date,
    index_code,
    open, high, low, close,
    volume, amount,

    -- 用 trade_date 生成日期键
    to_number(to_char(trade_date, 'YYYYMMDD')) as date_key,

    -- 生成一个唯一键，方便建 unique test
    index_code || '_' || to_char(trade_date, 'YYYYMMDD') as unique_key

from raw