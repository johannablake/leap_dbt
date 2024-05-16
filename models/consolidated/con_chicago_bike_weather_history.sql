-- define as table
{{ config(materialized="table", schema = "PROD_CONSOLIDATED") }}

select 
    *
from 
    prod_staging.stg_weather_history_day

