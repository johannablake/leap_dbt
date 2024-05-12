-- define as view
{{ config(materialized="view") }}

-- define columns 
{% set columns = adapter.get_columns_in_relation(
    source("analytics_raw", "chicago_bike_station_status_flatten_full")
) %}

-- cte for converting column names to lowercase
with
    lowercase_columns as (
        select
            {% for column in columns %}
                "{{ column.name }}" as "{{ column.name | lower }}"
                {% if not loop.last %}, {% endif %}
            {% endfor %}
        from {{ source("analytics_raw", "chicago_bike_station_status_flatten_full") }}
    )

select 
    "last_updated",
    "station_id",
    "num_ebikes_available",
    "num_ebikes_available_bool",
    "num_bikes_available",
    "num_docks_available",
    "last_reported",
    "station_status",
    "is_installed",
    "num_docks_disabled",
    "is_renting",
    "is_returning",
    "eightd_has_available_keys",
    "legacy_id"
from 
    lowercase_columns
