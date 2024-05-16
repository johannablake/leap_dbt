-- define as view
{{ config(materialized="view", schema = "STAGING") }}

-- define columns 
{% set columns = adapter.get_columns_in_relation(
    source("SNOWFLAKE", "CHICAGO_BIKE_STATION_STATUS_FLATTEN_FULL")
) %}


with source as(
    select * from {{ source('SNOWFLAKE', 'CHICAGO_BIKE_STATION_STATUS_FLATTEN_FULL') }}
),

renamed as (

    select
        last_updated,
        station_id,
        num_ebikes_available,
        num_ebikes_available_bool,
        num_bikes_available,
        num_docks_available,
        last_reported,
        station_status,
        is_installed,
        num_docks_disabled,
        num_bikes_disabled,
        is_renting,
        is_returning,
        eightd_has_available_keys,
        legacy_id

    from source

)

select * from renamed
