-- put this in macros/get_custom_schema.sql

{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}

        {{ default_schema }}

    {%- elif target.name == 'local' -%}

         {{ default_schema ~ '_' ~ custom_schema_name }}

    {%- else -%}

        {{ custom_schema_name }}

    {%- endif -%}

{%- endmacro %}