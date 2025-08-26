CREATE VIEW [{{mapping.EntityTarget.CodeModel}}].[vw_src_{{mapping.Name.replace(' ','_')}}] AS
SELECT
    {% for attr_mapping in mapping.AttributeMapping %}
        {% if 'Expression' in attr_mapping %}
            [{{attr_mapping.AttributeTarget.Code}}] = {{ attr_mapping.Expression }}(
                {{ attr_mapping.AttributesSource.EntityAlias }}.[{{attr_mapping.AttributesSource.Name}}]
            )
        {% else %}
            [{{attr_mapping.AttributeTarget.Code}}] = [{{ attr_mapping.AttributesSource.EntityAlias }}].[{{attr_mapping.AttributesSource.Code}}]
        {% endif %}
        {%- if not loop.last -%},
        {% endif %}
    {% endfor %}

    {% for sourceObject in mapping.SourceComposition %}
        {% if sourceObject.JoinType != 'APPLY' %}
            {{ sourceObject.JoinType }}
            [{{ sourceObject.Entity.CodeModel }}].[{{ sourceObject.Entity.Code }}] AS {{ sourceObject.JoinAlias }}

            {% if 'JoinConditions' in sourceObject %}
                ON {% for joinCondition in sourceObject.JoinConditions %}
                    {% if joinCondition.ParentLiteral != '' %}
                        {{ sourceObject.JoinAlias }}.[{{ joinCondition.JoinConditionComponents.AttributeChild.Code }}] = {{ joinCondition.ParentLiteral }}
                    {% else %}
                        {{ sourceObject.JoinAlias }}.[{{ joinCondition.JoinConditionComponents.AttributeChild.Code }}] = {{ joinCondition.JoinConditionComponents.AttributeParent.EntityAlias }}.[{{ joinCondition.JoinConditionComponents.AttributeParent.Code }}]
                    {% endif %}
                    {%- if not loop.last -%}
                        AND
                    {% endif %}
                {% endfor %}
            {% endif %}
        {% endif %}
    {% endfor %}
GROUP BY
    {% for attr_mapping in mapping.AttributeMapping %}
        {% if 'Expression' not in attr_mapping and not loop.first %},
            {{ attr_mapping.AttributesSource.EntityAlias }}.[{{attr_mapping.AttributesSource.Name}}] {% elif 'Expression' not in attr_mapping %}
            {{ attr_mapping.AttributesSource.EntityAlias }}.[{{attr_mapping.AttributesSource.Name}}]
        {% endif %}
    {% endfor %};
