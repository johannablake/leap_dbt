-- define as view
{{ config(materialized="view") }}

-- define columns 
{% set columns = adapter.get_columns_in_relation(ref('r_weather_history_day')) %}
