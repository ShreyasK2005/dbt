-- Macro to build base tables within integration layer
-- Using macros helps speed up ODS development in which most tables will follow similar patterns
-- key_column_list can either be a string for a single key like 'cust_pk' or a list like ['order_id', 'line_id'] if there is a composite key

{% macro ods_base_table(source_system, source_table, key_column_list) -%}

    {{ config(
        alias=get_model_alias(this.name,2)
    ) }}

    with src as
    (
        SELECT *
        from {{ source(source_system, source_table) }}
    )

    select *,
        {{ ods_build_change_hash(source_system, source_table, key_column_list) }} as change_hash,
        sysdate() as ods_create_datetime,
        sysdate() as ods_update_datetime
    from src

{%- endmacro %}