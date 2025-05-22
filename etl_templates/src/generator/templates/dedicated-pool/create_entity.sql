{#set variables with namespace for using in the whole code #}
{%- set entPkey = namespace(value="") -%}
{%- set attrPkey = namespace(value="FALSE") -%}
{%- set stereotypeFlag = namespace(value='FALSE') -%} 
{%- if Attributes in entity -%}
{%- for Attribute in entity.Attributes -%}  
{%- set entPkey.value = entity.Code ~ 'Key' -%} 
{#Check for existing primary key #}
{%- if Attribute.Code == entPkey.value -%} {%- set attrPkey.value = 'TRUE' -%}
{%- endif -%}{%- endfor -%}
{%- elif Attributes not in entity -%}
{%- set entPkey.value = entity.Code  ~ 'Key'-%} {%- set attrPkey.value =  'FALSE' -%}
{%- endif -%}   
{%- if entity.Stereotype-%}
{%- set stereotypeFlag.value = 'TRUE'-%}
{%- endif -%}
CREATE TABLE [{{entity.CodeModel}}].[{{entity.Code}}]
(
{% if attrPkey.value == 'FALSE' %} [{{entPkey.value}}]  bigint IDENTITY(1,1) NOT NULL
{% endif %}
{%- for Attribute in entity.Attributes -%}
{% if Attribute.Code == entPkey.value %} [{{Attribute.Code}}]   bigint IDENTITY(1,1) NOT NULL 
{%endif%}
{% endfor %}
{%- for Identifier in entity.Identifiers -%}
    ,{{Identifier}} 
{% endfor %} 
{%- for Attribute in entity.Attributes -%}
{%- if Attribute.Code != entPkey.value -%}
 ,[{{Attribute.Code}}]  {% if Attribute.DataType[:1] == 'N' %}
numeric({{Attribute.Length}},{% if Attribute.Precision%}{{Attribute.Precision}}{% else%}0{%endif%})
{% elif Attribute.DataType[:2] == 'DC' %}
decimal({{Attribute.Length}},{% if Attribute.Precision%}{{Attribute.Precision}}{% else%}0{%endif%})
{% elif Attribute.DataType[:1] == 'F'  %} 
float({{Attribute.Length}})
{% elif Attribute.DataType[:2] == 'SF' %}
float(24)
{% elif Attribute.DataType[:2] == 'LF' %}
float(53)
{% elif Attribute.DataType[:2] == 'MN' %}
decimal(28,4)
{% elif Attribute.DataType[:2] == 'NO' %}
bigint
{% elif Attribute.DataType[:1] == 'A' %}
nchar({{Attribute.Length}})
{% elif Attribute.DataType[:2] == 'VA' %}
nvarchar({{Attribute.Length}})
{% elif Attribute.DataType[:2] == 'BT' %}
nchar({{Attribute.Length}})
{% elif Attribute.DataType[:3] == 'MBT' %}
nchar({{Attribute.Length}})
{% elif Attribute.DataType[:4] == 'VMBT' %}
nvarchar({{Attribute.Length}})
{% elif Attribute.DataType[:2] == 'LA': %}
nchar({{Attribute.Length}})
{% elif Attribute.DataType[:3] == 'LVA': %}
nvarchar({{Attribute.Length}})
{% elif Attribute.DataType[:3] == 'TXT' %}
nvarchar({{Attribute.Length}})
{% elif Attribute.DataType[:3] == 'BIN' %}
binary({{Attribute.Length}})
{% elif Attribute.DataType[:4] == 'VBIN' %}
varbinary({{Attribute.Length}})
{% elif Attribute.DataType[:4] == 'LBIN' %}
varbinary(max)
{% elif Attribute.DataType[:2] == 'DT' %}
datetime
{% elif Attribute.DataType[:1] == 'D' %}
date
{% elif Attribute.DataType[:2] == 'BL' %}
bit
{% elif Attribute.DataType[:1] == 'I' %}
int
{% elif Attribute.DataType[:2] == 'SI' %}
smallint
{% elif Attribute.DataType[:2] == 'LI'%}
int
{% else %}
{{Attribute.DataType}}
{%endif%} {% endif %}{%- endfor -%}
{%- if stereotypeFlag.value ==  "FALSE" -%}
,[X_Startdate]    date
,[X_EndDate]  date
,[X_HashKey]  varbinary(8000)  
,[X_IsCurrent]    bit
,[X_IsReplaced]   bit
,[X_RunId]    int
,[X_LoadDateTime] datetime
,[X_Bron] nvarchar(10)
{% endif %}
)

WITH
(
    {% if entity.Number|int < 10000000 %}
    DISTRIBUTION = ROUND_ROBIN,
    HEAP
{% elif entity.Number|int >= 10000000 and entity.Number|int < 60000000 %}
    DISTRIBUTION = ROUND_ROBIN,
    CLUSTERED COLUMNSTORE INDEX
{% elif entity.Number|int >= 60000000 %}
    {% if stereotypeFlag.value == 'FALSE'%}DISTRIBUTION = HASH([{{entity.Code}}BKey]),
    {% elif stereotypeFlag.value == 'TRUE'%}DISTRIBUTION = HASH({% for Attribute in entity.Attributes%}[{{Attribute.Code}}] {%- if not loop.last -%}, {% endif %} {% endfor %}), 
    {% endif %}
    CLUSTERED COLUMNSTORE INDEX
    {% endif %}
);