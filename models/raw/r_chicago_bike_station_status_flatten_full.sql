-- define as view
{{ config(materialized="view", schema = "RAW") }}

-- define columns 
{% set columns = adapter.get_columns_in_relation(
    source("ANALYTICS_RAW", "CHICAGO_BIKE_STATION_STATUS_FLATTEN_FULL")
) %}

-- cte for converting column names to lowercase
with
    lowercase_columns as (
        select
            {% for column in columns %}
                "{{ column.name }}" as "{{ column.name | lower }}"
                {% if not loop.last %}, {% endif %}
            {% endfor %}
        from {{ source("ANALYTICS_RAW", "CHICAGO_BIKE_STATION_STATUS_FLATTEN_FULL") }}
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
