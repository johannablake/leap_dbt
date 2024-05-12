-- define as view
{{ config(materialized="view", schema = "RAW") }}

-- define columns 
{% set columns = adapter.get_columns_in_relation(
    source("ANALYTICS_RAW", "CHICAGO_BIKE_STATION_FLATTEN")
) %}

-- cte for converting column names to lowercase
with
    lowercase_columns as (
        select
            {% for column in columns %}
                "{{ column.name }}" as "{{ column.name | lower }}"
                {% if not loop.last %}, {% endif %}
            {% endfor %}
        from {{ source("ANALYTICS_RAW", "CHICAGO_BIKE_STATION_FLATTEN") }}
    )

select 
    "short_name",
    "station_type",
    "name",
    "lon",
    "electric_bike_surcharge_waiver",
    "external_id",
    "legacy_id",
    "capacity",
    "has_kiosk",
    "station_id",
    "region_id",
    "eightd_station_services",
    "lat"
from 
    lowercase_columns
