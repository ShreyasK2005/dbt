-- Parses the alias from the fully qualified dbt model name
-- name_part is the index of the model name containing the string to use for the alias
-- For example, in "ods__erp__customer" the name "customer" is index position 2 (ods is 0, erp is 1)

{%- macro get_model_alias(model_name, name_part) -%}
    {% set parts = model_name.split('__') %}
    {{ parts[name_part] }}
{%- endmacro -%}