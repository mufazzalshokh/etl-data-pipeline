/*
  Silver layer — Country dimension table.

  Transformations applied:
    - Deduplication: keep the most recently extracted record per cca2
    - Null handling: normalize missing values to NULL (not empty strings)
    - Derived columns: population_tier, has_multiple_currencies

  This table is the authoritative reference for country data across
  all downstream Gold models and the dashboard.

  Materialization: table (Silver is always materialized for join performance)
*/

with staged as (
    select * from {{ ref('stg_countries') }}
),

-- Deduplication: if a country was extracted multiple times, keep newest
deduplicated as (
    select *,
        row_number() over (
            partition by cca2
            order by extracted_at desc
        ) as row_num
    from staged
),

cleaned as (
    select
        -- Identity
        cca2,
        cca3,

        -- Names — normalize empty string → NULL
        nullif(trim(name_common), '')   as name_common,
        nullif(trim(name_official), '') as name_official,

        -- Geography
        nullif(trim(region), '')        as region,
        nullif(trim(subregion), '')     as subregion,
        nullif(trim(capital), '')       as capital,

        -- Demographics
        population,
        area_km2,

        -- Derived: population tier for analytics grouping
        case
            when population >= 1000000000 then 'mega'       -- 1B+
            when population >= 100000000  then 'large'      -- 100M+
            when population >= 10000000   then 'medium'     -- 10M+
            when population >= 1000000    then 'small'      -- 1M+
            when population is not null   then 'micro'      -- <1M
            else null
        end as population_tier,

        -- Derived: population density (people per km²)
        case
            when area_km2 > 0 then round(population::decimal / area_km2, 2)
            else null
        end as population_density,

        -- Currencies & languages
        currencies,
        languages,

        -- Derived: does the country use multiple currencies?
        case
            when currencies like '%,%' then true
            else false
        end as has_multiple_currencies,

        -- Governance
        is_un_member,
        timezones,
        flag_emoji,

        -- Lineage
        source_system,
        extracted_at,
        pipeline_run_id

    from deduplicated
    where row_num = 1
)

select * from cleaned