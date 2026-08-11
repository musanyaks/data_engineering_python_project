{% macro cents_to_dollars(column_name, precision=2) -%}
    ROUND({{ column_name }} / 100, {{ precision }})
{%- endmacro %}
