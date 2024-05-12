-- define as view
{{ config(materialized="view") }}

-- define columns 
{% set column_names = get_columns('r_weather_history_day') %}

SELECT
    {% for column in column_names %}
        "{{ column }}" AS "{{ column | lower }}"
        {% if not loop.last %}, {% endif %}
    {% endfor %}
FROM {{ ref('r_weather_history_day') }}

