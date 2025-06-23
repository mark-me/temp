{# set variables WITH namespace for USING IN THE whole code #} {%- set entPkey = namespace(
    value = ""
) -%}
{%- set attrPkey = namespace(
    value = "FALSE"
) -%}
{%- set stereotypeFlag = namespace(
    value = 'FALSE'
) -%}
{%- if Attributes in entity -%}
    {%- for Attribute in entity.Attributes -%}
        {%- set entPkey.value = entity.Code ~ 'Key' -%}
        {# CHECK for existing primary key #} {%- if Attribute.Code == entPkey.value -%}
            {%- set attrPkey.value = 'TRUE' -%}
        {%- endif -%}
    {%- endfor -%}

    {%- elif Attributes not in entity -%}
    {%- set entPkey.value = entity.Code ~ 'Key' -%}
    {%- set attrPkey.value = 'FALSE' -%}
{%- endif -%}

{%- if entity.Stereotype -%}
    {%- set stereotypeFlag.value = 'TRUE' -%}
{%- endif -%}

CREATE TABLE [{{entity.CodeModel}}].[{{entity.Code}}] ({% if attrPkey.value == 'FALSE' %}
    [{{entPkey.value}}] bigint IDENTITY(1, 1) NOT NULL
{% endif %}

{%- for Attribute in entity.Attributes -%}
    {% if Attribute.Code == entPkey.value %}
        [{{Attribute.Code}}] bigint IDENTITY(1, 1) NOT NULL
    {% endif %}
{% endfor %}

{%- for Identifier in entity.Identifiers -%}, {{ Identifier }}
{% endfor %}

{%- for Attribute in entity.Attributes -%}
    {%- if Attribute.Code != entPkey.value -%}, [{{Attribute.Code}}] {{ Attribute.DataTypeSQL }}
    {% endif %}
{%- endfor -%}

{%- if stereotypeFlag.value == "FALSE" -%}, [X_Startdate] DATE, [X_EndDate] DATE, [X_HashKey] INT, [X_IsCurrent] bit, [X_IsReplaced] bit, [X_RunId] NVARCHAR(100), [X_LoadDateTime] datetime, [X_Bron] nvarchar(10)
{% endif %}) WITH ({% if entity.Number | int < 10000000 %}
    DISTRIBUTION = ROUND_ROBIN, HEAP {% elif entity.Number | int >= 10000000 and entity.Number | int < 60000000 %}
    DISTRIBUTION = ROUND_ROBIN, CLUSTERED COLUMNSTORE INDEX {% elif entity.Number | int >= 60000000 %}
    {% if stereotypeFlag.value == 'FALSE' %}
        DISTRIBUTION = HASH([{{entity.Code}}BKey]), {% elif stereotypeFlag.value == 'TRUE' %}
        DISTRIBUTION = HASH({% for Attribute in entity.Attributes %}
            [{{Attribute.Code}}] {%- if not loop.last -%},
            {% endif %}
        {% endfor %}),
    {% endif %}

    CLUSTERED COLUMNSTORE INDEX
{% endif %});
