
  
    

create or replace transient table FIVETRAN_DATABASE.MLDS430_KOALA_INDEX_RAW.fact_index_daily
    
    
    
    as (

-- Start with clean daily data from stg_index_daily and compute prior close
with base as (

    select
        s.date_key,
        s.trade_date,
        s.index_code,
        s.open, s.high, s.low, s.close,
        s.volume, s.amount,

        lag(s.close) over (
            partition by s.index_code
            order by s.trade_date
        ) as prev_close

    from FIVETRAN_DATABASE.MLDS430_KOALA_INDEX_RAW.stg_index_daily s
),

-- Compute daily returns and rolling metrics based on base
returns as (

    select
        *,
        case 
            when prev_close is null or prev_close = 0 then 0
            else (close / prev_close) - 1
        end as daily_return,

        -- Log cumulative return (accumulate log(1+r), then exp back later)
        sum(
            ln(
                1 + case
                        when prev_close is null or prev_close = 0 then 0
                        else (close / prev_close) - 1
                    end
            )
        ) over (
            partition by index_code
            order by trade_date
        ) as log_cumret,

        -- 20-day rolling average return
        avg(
            case 
                when prev_close is null or prev_close = 0 then 0
                else (close / prev_close) - 1
            end
        ) over (
            partition by index_code
            order by trade_date
            rows between 19 preceding and current row
        ) as rolling_20d_avg_return,

        -- 20-day rolling volatility (standard deviation)
        stddev_samp(
            case 
                when prev_close is null or prev_close = 0 then 0
                else (close / prev_close) - 1
            end
        ) over (
            partition by index_code
            order by trade_date
            rows between 19 preceding and current row
        ) as rolling_20d_vol

    from base
)

select
    r.date_key,
    r.trade_date,
    r.index_code,
    r.open, r.high, r.low, r.close,
    r.volume, r.amount,

    r.daily_return,
    exp(r.log_cumret) - 1 as cumulative_return,   -- Convert log cumulative return back to normal cumulative return
    r.rolling_20d_avg_return,
    r.rolling_20d_vol,

    -- Join to the date dimension to add year / month / weekday
    d.year,
    d.month,
    d.quarter,
    d.weekday_short,
    d.weekday_full

from returns r
left join FIVETRAN_DATABASE.MLDS430_KOALA_INDEX_RAW.dim_date d
  on r.date_key = d.date_key
    )
;


  