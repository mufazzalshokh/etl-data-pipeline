/*
  Gold layer — Latest currency rate summary.

  Business question: What are the current exchange rates for all major
  currencies, with context on recent volatility?

  Grain: one row per target_currency (latest available date)

  Powers: the main dashboard currency table.

  Materialization: table
*/

with rates as (
    select * from {{ ref('fct_exchange_rates') }}
),

-- Latest rate per currency
latest_rates as (
    select *,
        row_number() over (
            partition by base_currency, target_currency
            order by rate_date desc
        ) as recency_rank
    from rates
),

-- 30-day volatility: standard deviation of rates
volatility as (
    select
        base_currency,
        target_currency,
        stddev(rate)                             as rate_stddev_30d,
        avg(rate)                                as rate_avg_30d,
        min(rate)                                as rate_min_30d,
        max(rate)                                as rate_max_30d,
        count(*)                                 as days_of_data
    from rates
    where rate_date >= current_date - interval '30 days'
    group by base_currency, target_currency
)

select
    lr.base_currency,
    lr.target_currency,
    lr.rate                                       as latest_rate,
    lr.inverse_rate                               as inverse_rate,
    lr.rate_date                                  as rate_date,
    lr.rate_change_pct                            as day_over_day_change_pct,
    lr.currency_region                            as region,
    lr.primary_country_name,

    -- 30-day window metrics
    round(v.rate_avg_30d::decimal, 6)             as rate_avg_30d,
    round(v.rate_min_30d::decimal, 6)             as rate_min_30d,
    round(v.rate_max_30d::decimal, 6)             as rate_max_30d,
    round(v.rate_stddev_30d::decimal, 8)          as rate_volatility_30d,

    -- Volatility tier: low / medium / high
    case
        when v.rate_stddev_30d / nullif(v.rate_avg_30d, 0) < 0.01  then 'low'
        when v.rate_stddev_30d / nullif(v.rate_avg_30d, 0) < 0.05  then 'medium'
        else 'high'
    end                                           as volatility_tier,

    v.days_of_data,
    lr.pipeline_run_id

from latest_rates lr
left join volatility v
    using (base_currency, target_currency)
where lr.recency_rank = 1
order by lr.target_currency