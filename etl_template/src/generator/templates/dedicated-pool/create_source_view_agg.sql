CREATE VIEW [{{mapping.EntityTarget.CodeModel}}].[vw_src_{{mapping.Name.replace(' ','_')}}]
AS SELECT
{% set sqlexpression = {
    'AVERAGE': 'AVG',
    'COUNT': 'COUNT',
    'MAXIMUM': 'MAX',
    'MINIMUM': 'MIN',
    'SUM': 'SUM' } -%}
{% for attributemapping in mapping.AttributeMapping %}
{% if 'Expression' in attributemapping %}[{{attributemapping.AttributeTarget.Code}}]  = {{sqlexpression.get(attributemapping.Expression)}}({{attributemapping.AttributesSource.IdEntity}}.[{{attributemapping.AttributesSource.Name}}])
{% elif CodeModel|upper == 'DA_CENTRAL' and Expression not in attributemapping %}[{{attributemapping.AttributeTarget.Code}}]  = {{attributemapping.AttributesSource.IdEntity}}.[{{attributemapping.AttributesSource.Code}}]
{% elif CodeModel|upper != 'DA_CENTRAL' and Expression not in attributemapping %}[{{attributemapping.AttributeTarget.Code}}]  = {{attributemapping.AttributesSource.IdEntity}}.[{{attributemapping.AttributesSource.Name}}]
{% endif %}
{%- if not loop.last -%}, {% endif %}
{% endfor %}
{% for sourceObject in mapping.SourceComposition %}
{% if sourceObject.JoinType != 'APPLY' %}{{ sourceObject.JoinType}} {{sourceObject.Entity.CodeModel}}.{{sourceObject.Entity.Name}} AS {{sourceObject.Entity.Id}} 
{% if 'JoinConditions' in sourceObject %}ON 
{% for joinCondition in sourceObject.JoinConditions %}
{% if CodeModel|upper == 'DA_CENTRAL' and joinCondition.ParentLiteral != '' %}{{sourceObject.JoinAlias}} .{{joinCondition.JoinConditionComponents.AttributeChild.Code}} = {{joinCondition.ParentLiteral}}
{% elif CodeModel|upper != 'DA_CENTRAL' and joinCondition.ParentLiteral != '' %}{{sourceObject.JoinAlias}} .{{joinCondition.JoinConditionComponents.AttributeChild.Name}} = {{joinCondition.ParentLiteral}}
{% endif %}
{% if CodeModel|upper == 'DA_CENTRAL' and joinCondition.ParentLiteral == '' %}{{sourceObject.JoinAlias}} .{{joinCondition.JoinConditionComponents.AttributeChild.Code}} = {{joinCondition.JoinConditionComponents.AttributeParent.EntityAlias}}.{{joinCondition.JoinConditionComponents.AttributeParent.Code}} 
{% elif CodeModel|upper != 'DA_CENTRAL' and joinCondition.ParentLiteral == '' %}{{sourceObject.JoinAlias}} .{{joinCondition.JoinConditionComponents.AttributeChild.Name}} = {{joinCondition.JoinConditionComponents.AttributeParent.EntityAlias}}.{{joinCondition.JoinConditionComponents.AttributeParent.Name}} 
{% endif %}
{%- if not loop.last -%} AND {% endif %}
{% endfor %}
{% endif %}
{% endif %}    
{% endfor %}
GROUP BY
{% for attributemapping in mapping.AttributeMapping %}
{% if 'Expression' not in attributemapping and not loop.first%},{{attributemapping.AttributesSource.IdEntity}}.[{{attributemapping.AttributesSource.Name}}]
{% elif 'Expression' not in attributemapping%}{{attributemapping.AttributesSource.IdEntity}}.[{{attributemapping.AttributesSource.Name}}]
{% endif %}
{% endfor %}
;
