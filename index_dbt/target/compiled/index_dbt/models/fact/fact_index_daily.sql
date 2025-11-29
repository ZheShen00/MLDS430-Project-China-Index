

-- 先从 stg_index_daily 拿到干净日线数据，并算前一日收盘价
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

-- 在 base 的基础上计算日收益和滚动指标
returns as (

    select
        *,
        case 
            when prev_close is null or prev_close = 0 then 0
            else (close / prev_close) - 1
        end as daily_return,

        -- 对数累计收益（先累积 log(1+r)，后面再 exp 回来）
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

        -- 20 日滚动平均收益
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

        -- 20 日滚动波动率（标准差）
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
    exp(r.log_cumret) - 1 as cumulative_return,   -- 把 log 累计收益转回普通累计收益
    r.rolling_20d_avg_return,
    r.rolling_20d_vol,

    -- 连接日期维度，补充 year / month / weekday
    d.year,
    d.month,
    d.quarter,
    d.weekday_short,
    d.weekday_full

from returns r
left join FIVETRAN_DATABASE.MLDS430_KOALA_INDEX_RAW.dim_date d
  on r.date_key = d.date_key