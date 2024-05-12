{% macro get_columns(schema_name, table_name) %}
    {% set query %}
        SELECT column_name
        FROM "{{ schema_name }}".information_schema.columns
        WHERE table_name = '{{ table_name }}'
    {% endset %}

    {% set results = run_query(query) %}
    {% if execute %}
        {% set column_names = results.columns[0].values() %}
        {{ return(column_names) }}
    {% else %}
        {{ return([]) }}
    {% endif %}
{% endmacro %}
