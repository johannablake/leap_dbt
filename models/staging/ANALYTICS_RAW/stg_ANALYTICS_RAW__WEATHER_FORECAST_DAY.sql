with 

source as (

    select * from {{ source('ANALYTICS_RAW', 'WEATHER_FORECAST_DAY') }}

),

renamed as (

    select
        postal_code,
        country,
        time_init_utc,
        date_valid_std,
        doy_std,
        min_temperature_air_2m_f,
        avg_temperature_air_2m_f,
        max_temperature_air_2m_f,
        min_temperature_wetbulb_2m_f,
        avg_temperature_wetbulb_2m_f,
        max_temperature_wetbulb_2m_f,
        min_temperature_dewpoint_2m_f,
        avg_temperature_dewpoint_2m_f,
        max_temperature_dewpoint_2m_f,
        min_temperature_feelslike_2m_f,
        avg_temperature_feelslike_2m_f,
        max_temperature_feelslike_2m_f,
        min_temperature_windchill_2m_f,
        avg_temperature_windchill_2m_f,
        max_temperature_windchill_2m_f,
        min_temperature_heatindex_2m_f,
        avg_temperature_heatindex_2m_f,
        max_temperature_heatindex_2m_f,
        min_humidity_relative_2m_pct,
        avg_humidity_relative_2m_pct,
        max_humidity_relative_2m_pct,
        min_humidity_specific_2m_gpkg,
        avg_humidity_specific_2m_gpkg,
        max_humidity_specific_2m_gpkg,
        min_pressure_2m_mb,
        avg_pressure_2m_mb,
        max_pressure_2m_mb,
        min_pressure_mean_sea_level_mb,
        avg_pressure_mean_sea_level_mb,
        max_pressure_mean_sea_level_mb,
        min_wind_speed_10m_mph,
        avg_wind_speed_10m_mph,
        max_wind_speed_10m_mph,
        avg_wind_direction_10m_deg,
        min_wind_speed_80m_mph,
        avg_wind_speed_80m_mph,
        max_wind_speed_80m_mph,
        avg_wind_direction_80m_deg,
        min_wind_speed_100m_mph,
        avg_wind_speed_100m_mph,
        max_wind_speed_100m_mph,
        avg_wind_direction_100m_deg,
        tot_precipitation_in,
        tot_snowfall_in,
        min_cloud_cover_tot_pct,
        avg_cloud_cover_tot_pct,
        max_cloud_cover_tot_pct,
        min_radiation_solar_total_wpm2,
        avg_radiation_solar_total_wpm2,
        max_radiation_solar_total_wpm2,
        tot_radiation_solar_total_wpm2,
        probability_of_precipitation_pct,
        probability_of_snow_pct

    from source

)

select * from renamed
