/*
  Gold layer — Regional currency rate aggregation.

  Business question: How do exchange rates compare across geographic regions,
  and how have they trended over the past 30 days?

  Grain: one row per (region, target_currency, rate_date)

  Powers: the regional trends dashboard section.

  Materialization: table
*/

with rates as (
    select * from {{ ref('fct_exchange_rates') }}
),

countries as (
    select * from {{ ref('dim_countries') }}
),

-- Aggregate rates by region per day
regional_daily as (
    select
        r.rate_date,
        r.base_currency,
        r.target_currency,
        coalesce(r.currency_region, 'Unknown')   as region,
        r.primary_country_name,

        -- Rate statistics per region/currency/day
        avg(r.rate)                              as avg_rate,
        min(r.rate)                              as min_rate,
        max(r.rate)                              as max_rate,

        -- Average daily change for this region/currency
        avg(r.rate_change_pct)                   as avg_rate_change_pct,

        -- Count of countries in this region using this currency
        count(distinct c.cca2)                   as country_count

    from rates r
    left join countries c
        on c.region = r.currency_region
        and c.currencies like '%' || r.target_currency || '%'

    where r.rate_date >= current_date - interval '30 days'

    group by
        r.rate_date,
        r.base_currency,
        r.target_currency,
        coalesce(r.currency_region, 'Unknown'),
        r.primary_country_name
)

select
    rate_date,
    base_currency,
    target_currency,
    region,
    primary_country_name,
    round(avg_rate::decimal, 6)              as avg_rate,
    round(min_rate::decimal, 6)              as min_rate,
    round(max_rate::decimal, 6)              as max_rate,
    round(avg_rate_change_pct::decimal, 4)   as avg_rate_change_pct,
    country_count,

    -- 7-day rolling average for trend smoothing
    round(
        avg(avg_rate) over (
            partition by base_currency, target_currency, region
            order by rate_date
            rows between 6 preceding and current row
        )::decimal,
        6
    ) as rate_7d_moving_avg

from regional_daily
order by rate_date desc, target_currency, region