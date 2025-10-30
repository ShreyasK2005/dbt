-- Macro to generate dimension key values for lookup in fact tables
-- SHA2 hash of source system and business keys

{% macro dw_dim_key_lkp(source_system, key_columns) -%}

    {% if key_columns is string %}

        {{ "sha2(coalesce(cast(" ~ source_system ~ " as varchar), '') || '-' || coalesce(cast(" ~ key_columns ~ " as varchar), ''))" }}

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

        {{ "sha2(" ~ dbt.concat(fields) ~ ")" }}


    {% else %}

        {{ exceptions.raise_compiler_error("Unexpected values in key_column_list") }}

    {% endif %}

{%- endmacro %}