CREATE TABLE [{{entity.CodeModel}}].[{{entity.Code}}] (
    [{{entity.Code}}Key] bigint IDENTITY(1, 1) NOT NULL

{%- for Attribute in entity.Attributes -%}
    , [{{Attribute.Code}}] {{ Attribute.DataTypeSQL }}
{%- endfor -%}

{%- if entity.type_entity == "Regular" -%},
[X_Startdate] DATE, [X_EndDate] DATE, [X_HashKey] INT, [X_IsCurrent] bit, [X_IsReplaced] bit, [X_RunId] NVARCHAR(100), [X_LoadDateTime] datetime, [X_Bron] nvarchar(10),
CONSTRAINT [{{entity.Code}}_PK] PRIMARY KEY NONCLUSTERED (
    [{{entity.Code}}Key]
)NOT ENFORCED
,CONSTRAINT [{{entity.Code}}_UK] UNIQUE NONCLUSTERED(
    {% for KeyAttribute in entity.KeyPrimary.Attributes %}
        [{{KeyAttribute.Code}}]{%- if not loop.last -%},{% endif %}
    {%- endfor -%}
) NOT ENFORCED
{% endif %}
) WITH (
    {% if entity.Number | int < 10000000 %}
        DISTRIBUTION = ROUND_ROBIN, HEAP
    {% elif entity.Number | int >= 10000000 and entity.Number | int < 60000000 %}
    DISTRIBUTION = ROUND_ROBIN, CLUSTERED COLUMNSTORE INDEX {% elif entity.Number | int >= 60000000 %}
    {% if entity.type_entity == "Regular" %}
        DISTRIBUTION = HASH(
            {% for KeyAttribute in entity.KeyPrimary.Attributes %}
                [{{KeyAttribute.Code}}]{%- if not loop.last -%},{% endif %}
            {%- endfor -%}
            ),
        {% elif entity.type_entity == "Aggregate" %}
            DISTRIBUTION = HASH(
                {% for Attribute in entity.Attributes %}
                    [{{Attribute.Code}}] {%- if not loop.last -%},
                    {% endif %}
                {% endfor %}),
    {% endif %}

    CLUSTERED COLUMNSTORE INDEX
{% endif %});
