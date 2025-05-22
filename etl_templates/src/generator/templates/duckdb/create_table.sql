CREATE TABLE {{schema.name}}.{{table.name}}
(
{% for column in columns %}
    {{column.name}} {{column.data_type}} {% if column.nullable %} NULL {% else %} NOT NULL {% endif %}
    {%- if not loop.last -%}
        ,
    {% endif %}
{% endfor %}
);