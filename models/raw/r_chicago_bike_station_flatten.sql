-- define as view
{{ config(materialized="view") }}

-- define columns 
{% set columns = adapter.get_columns_in_relation(
    source("analytics_raw", "chicago_bike_station_flatten")
) %}

-- cte for converting column names to lowercase
with
    lowercase_columns as (
        select
            {% for column in columns %}
                "{{ column.name }}" as "{{ column.name | lower }}"
                {% if not loop.last %}, {% endif %}
            {% endfor %}
        from {{ source("analytics_raw", "chicago_bike_station_flatten") }}
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
