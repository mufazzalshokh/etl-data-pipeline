/*
  Staging view over raw_currencies — bronze layer.

  Purpose:
    - Provides a stable dbt reference point so Silver models don't
      directly reference bronze.raw_currencies (which is written by Python).
    - Applies minimal aliasing to keep column names consistent.
    - NO business logic here — that belongs in Silver.

  Materialization: view (always reflects latest Bronze data)
*/

with source as (
    select * from {{ source('bronze', 'raw_currencies') }}
),

staged as (
    select
        base_currency,
        target_currency,
        cast(rate as decimal(20, 8))         as rate,
        cast(rate_date as date)              as rate_date,
        cast(timestamp as bigint)            as unix_timestamp,
        _source                              as source_system,
        cast(_extracted_at as timestamptz)   as extracted_at,
        _pipeline_run_id                     as pipeline_run_id
    from source
    -- Basic defensive filter: skip obviously corrupted rows
    where target_currency is not null
      and rate is not null
      and rate_date is not null
)

select * from staged