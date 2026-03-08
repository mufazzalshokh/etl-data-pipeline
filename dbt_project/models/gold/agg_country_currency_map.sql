/*
  Gold layer — Country ↔ Currency mapping with latest rate context.

  Business question: For each country, what currency do they use,
  and what is its latest exchange rate vs USD?

  Grain: one row per country (cca2)

  Powers: the world map / country drill-down in the dashboard.

  Materialization: table
*/

with countries as (
    select * from {{ ref('dim_countries') }}
),

latest_rates as (
    select
        target_currency,
        rate                as latest_rate,
        rate_date           as latest_rate_date,
        rate_change_pct     as day_change_pct,
        volatility_tier
    from {{ ref('agg_currency_summary') }}
),

-- One row per country — join on primary currency (first listed)
country_with_rate as (
    select
        c.cca2,
        c.cca3,
        c.name_common,
        c.region,
        c.subregion,
        c.capital,
        c.population,
        c.population_tier,
        c.population_density,
        c.area_km2,
        c.currencies,
        c.is_un_member,
        c.flag_emoji,

        -- Extract primary currency (first code in comma-separated list)
        split_part(c.currencies, ', ', 1)          as primary_currency,

        -- Join latest rate for the primary currency
        r.latest_rate,
        r.latest_rate_date,
        r.day_change_pct,
        r.volatility_tier

    from countries c
    left join latest_rates r
        on r.target_currency = split_part(c.currencies, ', ', 1)
)

select * from country_with_rate
order by population desc nulls last