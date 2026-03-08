/*
  Silver layer — Exchange rates fact table.

  Transformations applied:
    - Deduplication: one record per (base_currency, target_currency, rate_date)
    - Derived columns: inverse_rate, rate_vs_previous_day (% change)
    - Join with dim_countries to enrich with country/region context
    - Filter: exclude obviously bad rates (rate = 0, rate > 1,000,000)

  Grain: one row per currency pair per day.

  Materialization: table
*/

with staged as (
    select * from {{ ref('stg_currencies') }}
),

-- Deduplication: if same currency+date loaded multiple times, keep latest
deduplicated as (
    select *,
        row_number() over (
            partition by base_currency, target_currency, rate_date
            order by extracted_at desc
        ) as row_num
    from staged
),

cleaned as (
    select
        base_currency,
        target_currency,
        rate,
        rate_date,

        -- Inverse rate: units of base per 1 unit of target
        round(1.0 / nullif(rate, 0), 8)  as inverse_rate,

        -- Day-over-day change using window function
        lag(rate) over (
            partition by base_currency, target_currency
            order by rate_date
        )                                as previous_day_rate,

        case
            when lag(rate) over (
                partition by base_currency, target_currency
                order by rate_date
            ) is not null
            and lag(rate) over (
                partition by base_currency, target_currency
                order by rate_date
            ) != 0
            then round(
                (rate - lag(rate) over (
                    partition by base_currency, target_currency
                    order by rate_date
                )) / lag(rate) over (
                    partition by base_currency, target_currency
                    order by rate_date
                ) * 100,
                4
            )
            else null
        end                              as rate_change_pct,

        unix_timestamp,
        source_system,
        extracted_at,
        pipeline_run_id

    from deduplicated
    where row_num = 1
      and rate > 0
      and rate < 1000000     -- filter obviously corrupted rates
),

-- Enrich with country context via currency → country join
-- Note: one currency can belong to multiple countries (e.g., EUR → ~20 countries)
-- We take region from the most populous country using that currency
enriched as (
    select
        r.*,
        c.region                 as currency_region,
        c.name_common            as primary_country_name
    from cleaned r
    left join {{ ref('dim_countries') }} c
        on c.currencies like '%' || r.target_currency || '%'
        -- For multi-country currencies (EUR), rank by population and take #1
        and c.population = (
            select max(population)
            from {{ ref('dim_countries') }}
            where currencies like '%' || r.target_currency || '%'
        )
)

select * from enriched