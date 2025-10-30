{% macro use_wh(wh_name) %}
    {% if execute %}
        {% do run_query('use warehouse ' ~ wh_name) %}
    {% endif %}
{% endmacro %}