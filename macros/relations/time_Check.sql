{% macro build_join_sql(schema, left_table, right_table, left_col, right_col, schema2=None) %}
    {%- if schema2 is none %}
        {% set schema2 = schema1 %}
    {%- endif %}

    with left_table as (
        select * from {{ source(schema, left_table) }}
    ),
    right_table as (
        select * from {{ source(schema, right_table) }}
    )
    select *
    from left_table l
    join right_table r
        on l.{{ left_col }} = r.{{ right_col }}
{% endmacro %}
