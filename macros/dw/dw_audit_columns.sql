-- Generate standard audit columns

{% macro dw_audit_columns() -%}

    '{{ source_system }}' as dw_source_name,
    sysdate() as dw_created_datetime,
    sysdate() as dw_updated_datetime,
    false as dw_is_deleted_flag

{%- endmacro %}