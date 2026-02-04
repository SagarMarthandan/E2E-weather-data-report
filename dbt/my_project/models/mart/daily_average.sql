-- This model calculates the daily average temperature and wind speed for each city.
-- It aggregates data from the staging weather data model.

{{config(
    materialized='table',
)}}

select
    city,
    date(weather_time_local) as date, -- Extract the date from the local weather timestamp
    round(avg(temperature)::numeric, 2) as avg_temperature, -- Calculate average temperature rounded to 2 decimal places
    round(avg(wind_speed)::numeric, 2) as avg_wind_speed -- Calculate average wind speed rounded to 2 decimal places
from
    {{ref('stg_weather_data')}}
group by
    city, -- Group by city and date to get daily averages
    date(weather_time_local)
order by
    city, -- Order the results for easier readability
    date(weather_time_local)