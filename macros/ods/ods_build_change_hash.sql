-- Creates a hash of values based on all the columns in a table, minus columns in an exclusion list
-- Hash values are helpful in snapshots and other models where change detection functionality is required

{% macro ods_build_change_hash(source_system, source_table, key_column_list) -%}

    {% set uppercased_key_columns = [] %}

    {% if key_column_list is string %}

        {# Uncomment DEBUG messages to capture types and variable values if needed for testing #}
        {# {{ log("DEBUG - key_column_list is string", info=True) }} #}

        {% do uppercased_key_columns.append(key_column_list | upper) %}
        {# {{ log("DEBUG - uppercased_key_columns: " ~ uppercased_key_columns, info=True) }} #}

    {% elif key_column_list is sequence %}

        {% for column in key_column_list %}
            {% do uppercased_key_columns.append(column | upper) %}
        {% endfor %}

    {% else %}

        {{ exceptions.raise_compiler_error("Unexpected values in key_column_list") }}

    {% endif %}

    {%- set timestamp_columns = ['ODS_CREATE_DATETIME', 'ODS_UPDATE_DATETIME'] -%}

    {%- set all_columns = adapter.get_columns_in_relation(source(source_system, source_table)) -%}

    {%- set excluded_columns = uppercased_key_columns + timestamp_columns -%}

    {%- set filtered_columns = all_columns | map(attribute='name') | reject('in', excluded_columns) | list -%}

    {{ dbt_utils.generate_surrogate_key(filtered_columns) }}

{%- endmacro %}