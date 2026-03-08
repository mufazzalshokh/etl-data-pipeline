/*
  Staging view over raw_countries — bronze layer.

  Purpose:
    - Stable interface to the raw countries table for upstream Silver models.
    - Type-casts text columns to proper types.
    - No business logic, no joins, no aggregations.

  Materialization: view
*/

with source as (
    select * from {{ source('bronze', 'raw_countries') }}
),

staged as (
    select
        cca2,
        cca3,
        name_common,
        name_official,
        region,
        subregion,
        capital,
        cast(nullif(population, '')  as bigint)  as population,
        cast(nullif(area_km2, '')    as decimal(15, 4)) as area_km2,
        currencies,
        languages,
        timezones,
        case
            when lower(is_un_member) in ('true', '1', 't', 'yes') then true
            when lower(is_un_member) in ('false', '0', 'f', 'no') then false
            else null
        end                                               as is_un_member,
        flag_emoji,
        _source                                           as source_system,
        cast(_extracted_at as timestamptz)                as extracted_at,
        _pipeline_run_id                                  as pipeline_run_id
    from source
    where cca2 is not null and cca2 != ''
)

select * from staged