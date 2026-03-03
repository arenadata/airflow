{% macro generate_alias_name(custom_alias_name=none, node=none) -%}
    {%- if custom_alias_name is none -%}
        {{ node.name }}
    {%- else -%}
        {{ custom_alias_name | trim }}
    {%- endif -%}
{%- endmacro %}

{% macro cents_to_dollars(column_name, scale=2) %}
    ({{ column_name }} / 100)::numeric(16, {{ scale }})
{% endmacro %}
