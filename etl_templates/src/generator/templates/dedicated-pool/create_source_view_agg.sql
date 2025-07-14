CREATE VIEW [{{mapping.EntityTarget.CodeModel}}].[vw_src_{{mapping.Name.replace(' ','_')}}] AS
SELECT
    {% for attr_mapping in mapping.attr_mapping %}
        {% if 'Expression' in attr_mapping %}
            [{{attr_mapping.AttributeTarget.Code}}] = {{ attr_mapping.Expression }}(
                {{ attr_mapping.AttributesSource.EntityAlias }}.[{{attr_mapping.AttributesSource.Name}}]
            ) {% elif CodeModel | upper == 'DA_CENTRAL' and Expression not in attr_mapping %}
            [{{attr_mapping.AttributeTarget.Code}}] = [{{ attr_mapping.AttributesSource.EntityAlias }}].[{{attr_mapping.AttributesSource.Code}}] {% elif CodeModel | upper != 'DA_CENTRAL' and Expression not in attr_mapping %}
            [{{attr_mapping.AttributeTarget.Code}}] = [{{ attr_mapping.AttributesSource.EntityAlias }}].[{{attr_mapping.AttributesSource.Name}}]
        {% endif %}

        {%- if not loop.last -%},
        {% endif %}
    {% endfor %}

    {% for sourceObject in mapping.SourceComposition %}
        {% if sourceObject.JoinType != 'APPLY' %}
            {{ sourceObject.JoinType }}
            [{{ sourceObject.Entity.CodeModel }}].[{{ sourceObject.Entity.Name }}] AS {{ sourceObject.JoinAlias }}

            {% if 'JoinConditions' in sourceObject %}
                ON {% for joinCondition in sourceObject.JoinConditions %}
                    {% if CodeModel | upper == 'DA_CENTRAL' and joinCondition.ParentLiteral != '' %}
                        {{ sourceObject.JoinAlias }}.[{{ joinCondition.JoinConditionComponents.AttributeChild.Code }}] = {{ joinCondition.ParentLiteral }}

                        {% elif CodeModel | upper != 'DA_CENTRAL' and joinCondition.ParentLiteral != '' %}
                        {{ sourceObject.JoinAlias }}.[{{ joinCondition.JoinConditionComponents.AttributeChild.Name }}] = {{ joinCondition.ParentLiteral }}
                    {% endif %}

                    {% if CodeModel | upper == 'DA_CENTRAL' and joinCondition.ParentLiteral == '' %}
                        {{ sourceObject.JoinAlias }}.[{{ joinCondition.JoinConditionComponents.AttributeChild.Code }}] = {{ joinCondition.JoinConditionComponents.AttributeParent.EntityAlias }}.[{{ joinCondition.JoinConditionComponents.AttributeParent.Code }}]

                        {% elif CodeModel | upper != 'DA_CENTRAL' and joinCondition.ParentLiteral == '' %}
                        {{ sourceObject.JoinAlias }}.[{{ joinCondition.JoinConditionComponents.AttributeChild.Name }}] = {{ joinCondition.JoinConditionComponents.AttributeParent.EntityAlias }}.[{{ joinCondition.JoinConditionComponents.AttributeParent.Name }}]
                    {% endif %}

                    {%- if not loop.last -%}
                        AND
                    {% endif %}
                {% endfor %}
            {% endif %}
        {% endif %}
    {% endfor %}
GROUP BY
    {% for attr_mapping in mapping.attr_mapping %}
        {% if 'Expression' not in attr_mapping and not loop.first %},
            {{ attr_mapping.AttributesSource.EntityAlias }}.[{{attr_mapping.AttributesSource.Name}}] {% elif 'Expression' not in attr_mapping %}
            {{ attr_mapping.AttributesSource.EntityAlias }}.[{{attr_mapping.AttributesSource.Name}}]
        {% endif %}
    {% endfor %};
