-- define as view
{{ config(materialized='view', schema = 'STAGING') }}

-- define columns
{% set columns = adapter.get_columns_in_relation(source('SNOWFLAKE', 'WEATHER_HISTORY_DAY')) %}

with source as (
    select * from {{ source('SNOWFLAKE', 'WEATHER_HISTORY_DAY') }}
),

renamed_and_converted as (
    select
        {% for column in columns %}
            {% if column.name.endswith('_F') %}
                -- convert fahrenheit to celsius and rename
                ({{ column.name }} - 32) * 5.0/9.0 as {{ column.name[:-2] }}_C,
            {% elif column.name.endswith('_MPH') %}
                -- convert mph to kph and rename
                {{ column.name }} * 1.60934 as {{ column.name[:-4] }}_KPH,
            {% else %}
                -- Include the column as is
                {{ column.name }},
            {% endif %}
        {% endfor %}
    from source
)

select
    postal_code,
    country,
    date_valid_std,
    doy_std,
    min_temperature_air_2m_c,
    avg_temperature_air_2m_c,
    max_temperature_air_2m_c,
    min_temperature_wetbulb_2m_c,
    avg_temperature_wetbulb_2m_c,
    max_temperature_wetbulb_2m_c,
    min_temperature_dewpoint_2m_c,
    avg_temperature_dewpoint_2m_c,
    max_temperature_dewpoint_2m_c,
    min_temperature_feelslike_2m_c,
    avg_temperature_feelslike_2m_c,
    max_temperature_feelslike_2m_c,
    min_temperature_windchill_2m_c,
    avg_temperature_windchill_2m_c,
    max_temperature_windchill_2m_c,
    min_temperature_heatindex_2m_c,
    avg_temperature_heatindex_2m_c,
    max_temperature_heatindex_2m_c,
    min_humidity_relative_2m_pct,
    avg_humidity_relative_2m_pct,
    max_humidity_relative_2m_pct,
    min_humidity_specific_2m_gpkg,
    avg_humidity_specific_2m_gpkg,
    max_humidity_specific_2m_gpkg,
    min_pressure_2m_mb,
    avg_pressure_2m_mb,
    max_pressure_2m_mb,
    min_pressure_tendency_2m_mb,
    avg_pressure_tendency_2m_mb,
    max_pressure_tendency_2m_mb,
    min_pressure_mean_sea_level_mb,
    avg_pressure_mean_sea_level_mb,
    max_pressure_mean_sea_level_mb,
    min_wind_speed_10m_kph,
    avg_wind_speed_10m_kph,
    max_wind_speed_10m_kph,
    avg_wind_direction_10m_deg,
    min_wind_speed_80m_kph,
    avg_wind_speed_80m_kph,
    max_wind_speed_80m_kph,
    avg_wind_direction_80m_deg,
    min_wind_speed_100m_kph,
    avg_wind_speed_100m_kph,
    max_wind_speed_100m_kph,
    avg_wind_direction_100m_deg,
    tot_precipitation_in,
    tot_snowfall_in,
    tot_snowdepth_in,
    min_cloud_cover_tot_pct,
    avg_cloud_cover_tot_pct,
    max_cloud_cover_tot_pct,
    min_radiation_solar_total_wpm2,
    avg_radiation_solar_total_wpm2,
    max_radiation_solar_total_wpm2,
    tot_radiation_solar_total_wpm2
from 
    renamed_and_converted
