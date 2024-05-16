-- define as view
{{ config(materialized="view", schema = "PROD_STAGING") }}

-- define columns 
{% set columns = adapter.get_columns_in_relation (
    source("SNOWFLAKE", "CHICAGO_BIKE_STATION_INFO_FLATTEN")
) %}


with source as (
    select * from {{ source('SNOWFLAKE', 'CHICAGO_BIKE_STATION_INFO_FLATTEN') }}
),

renamed as (

    select
        short_name,
        station_type,
        name,
        lon,
        electric_bike_surcharge_waiver,
        external_id,
        legacy_id,
        capacity,
        has_kiosk,
        station_id,
        region_id,
        eightd_station_services,
        lat

    from source

)

select * from renamed
