{% macro generate_database_name(custom_database_name=none, node=none) -%}

    {%- set default_database = target.database -%}
    {%- if custom_database_name is none -%}

        {{ default_database }}

    {%- elif custom_database_name == 'SANDBOX' -%}

        {{ custom_database_name }}

    {%- else -%}

        {% if target.name == 'dev' or target.name == 'local' %}
            {{ 'DEV_' ~ custom_database_name }}
        {% elif target.name == 'test' %}
            {{ 'TEST_' ~ custom_database_name }}
        {% else %}
            {{ custom_database_name }}
        {% endif %}

    {%- endif -%}

{%- endmacro %}
