-- Macro to generate dimension surrogate key
-- SHA2 hash of source system and business keys

{% macro dw_create_dim_key(source_system, key_columns) -%}

    {% set dim_name = this.identifier | replace('dim_', '') %}
    {% set dim_key_name = dim_name ~ '_key' %}

    {% if key_columns is string %}

        {{ "sha2(coalesce(cast(" ~ source_system ~ " as varchar), '') || '-' || coalesce(cast(" ~ key_columns ~ " as varchar), '')) as " ~ dim_key_name }}

    {% elif key_columns is sequence %}

        {%- set fields = [] -%}
        {%- do fields.append(
            "coalesce(cast('" ~ source_system ~ "' as varchar), '')"
        ) -%}

        {%- do fields.append("'-'") -%}


        {%- for field in key_columns -%}

            {%- do fields.append(
                "coalesce(cast(" ~ field ~ " as varchar), '')"
            ) -%}

            {%- if not loop.last %}
                {%- do fields.append("'-'") -%}
            {%- endif -%}

        {%- endfor -%}

        {{ "sha2(" ~ dbt.concat(fields) ~ ") as " ~ dim_key_name }}


    {% else %}

        {{ exceptions.raise_compiler_error("Unexpected values in key_column_list") }}

    {% endif %}

{%- endmacro %}