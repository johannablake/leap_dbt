-- define as table
{{ config(materialized="table", schema = "PROD_CONSOLIDATED") }}

select 
    *
from 
    prod_staging.weather_history_day

