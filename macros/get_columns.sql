{% macro get_columns(model_name) %}
    {% set target_relation = ref(model_name) %}

    {% set query %}
        SELECT column_name
        FROM "{{ target_relation.database }}".information_schema.columns
        WHERE table_schema = '{{ target_relation.schema }}'
        AND table_name = '{{ target_relation.identifier }}'
    {% endset %}

    {% set results = run_query(query) %}
    {% if execute %}
        {% set column_names = results.columns[0].values() %}
        {{ return(column_names) }}
    {% else %}
        {{ return([]) }}
    {% endif %}
{% endmacro %}
