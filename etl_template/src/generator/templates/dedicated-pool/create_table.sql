CREATE TABLE [{{item.Schema}}].[{{item.Name}}]
(
    [{{item.Name}}Key] bigint IDENTITY(1,1) NOT NULL,
{% for column in item.Columns %}
    [{{column.Name}}] {{column.DataType}}
    {%- if not loop.last -%}
        ,
    {% endif %}
{% endfor %}

)
WITH
(
    {% if item.Rowcount|int < 10000000 %}
    DISTRIBUTION = ROUND_ROBIN,
    HEAP
    {% elif item.Rowcount|int >= 10000000 and item.Rowcount|int < 60000000 %}
    DISTRIBUTION = ROUND_ROBIN,
    CLUSTERED COLUMNSTORE INDEX
    {% elif item.Rowcount|int >= 60000000 %}
    DISTRIBUTION = HASH([" & "'" & idcolumnname & "'" & "],
    CLUSTERED COLUMNSTORE INDEX
    {% endif %}
);