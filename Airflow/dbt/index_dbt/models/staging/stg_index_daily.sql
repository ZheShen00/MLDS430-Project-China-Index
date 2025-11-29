{{ config(materialized='table') }}

with raw as (

    select
        -- The date column is an integer like 20190102; cast to string then to DATE
        to_date(to_char("DATE"), 'YYYYMMDD') as trade_date,

        -- table_name stores index IDs (1000001 etc.); cast to string index_code
        cast(table_name as varchar) as index_code,

        -- Map price and volume related fields
        cast(o as float) as open,
        cast(h as float) as high,
        cast(l as float) as low,
        cast(c as float) as close,
        cast(v as float) as volume,
        cast(a as float) as amount

    from {{ source('index_raw', 'INDEX_DAILY_2019_2023_FILTERED') }}

    -- To keep only certain indices, add a filter like where table_name in (...)
    -- where table_name in (1000001, 2399001, 2399006, ...)

)

select
    trade_date,
    index_code,
    open, high, low, close,
    volume, amount,

    -- Generate date_key from trade_date
    to_number(to_char(trade_date, 'YYYYMMDD')) as date_key,

    -- Build a unique key to support the unique test
    index_code || '_' || to_char(trade_date, 'YYYYMMDD') as unique_key

from raw
