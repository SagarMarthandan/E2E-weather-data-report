-- This model stages the raw weather data, performs deduplication, and calculates local timestamps.

{{ config(
    materialized='table',
    unique_key='id'
) }}

-- Fetch raw data from the 'dev' source
with source as (
    select 
        *
    from
        {{source('dev', 'raw_weather_data')}}
),

-- Deduplicate records based on the observation time, keeping the earliest insertion for each time slot
de_dup as (
    select
        *,
        row_number() over(partition by time order by inserted_at) as rn
    from
        source  
)

-- Final selection with column renaming and timestamp adjustments
select
    id,
    city, 
    temperature,
    weather_description,
    wind_speed,
    time as weather_time_local, -- Local time of observation as reported by the API
    -- Calculate the local insertion time by applying the UTC offset
    (inserted_at + (utc_offset || ' hours')::interval) as inserted_at_local 
from
    de_dup
where
    rn = 1