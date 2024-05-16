-- define as view
{{ config(materialized="table", schema = "CONSOLIDATED") }}

select 
    l.last_updated_date,
    k.temperature_avg,
    l.num_ebikes_available_avg,
    l.num_bikes_available_avg
from
(
    select 
        to_date(f.last_updated) as last_updated_date,
        round(avg(f.num_ebikes_available), 2) as num_ebikes_available_avg,
        round(avg(f.num_bikes_available), 2) as num_bikes_available_avg
    from
        prod_staging.stg_chicago_bike_station_status_flatten_full f
    where 
        year(f.last_updated) = 2022
    group by 
        to_date(f.last_updated)
) l
left join 
(
    select 
    h.date_valid_std,
    round(avg(h.avg_temperature_air_2m_c), 2) as temperature_avg
from 
    prod_staging.stg_weather_history_day h
where 
    h.country = 'US'
    and try_cast(h.postal_code as int) between 60018 and 60827
    and year(h.date_valid_std) = 2022
group by 
    h.date_valid_std
) k on l.last_updated_date = k.date_valid_std