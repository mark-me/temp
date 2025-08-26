CREATE VIEW [{{mapping.EntityTarget.CodeModel}}].[vw_src_{{mapping.Name}}] AS
SELECT
    {% for attributemapping in mapping.AttributeMapping %}
        {% if 'Expression' in attributemapping %}
        [{{ attributemapping.AttributeTarget.Code }}] = {{ attributemapping.Expression }},
        {% else %}
        [{{attributemapping.AttributeTarget.Code}}] = {{ attributemapping.AttributesSource.EntityAlias }}.[{{attributemapping.AttributesSource.Code}}],
        {% endif %}
    {% endfor %}
    [X_StartDate] = CAST(GETDATE() AT TIME ZONE 'UTC' AT TIME ZONE 'W. Europe Standard Time' AS DATE),
    [X_EndDate] = CAST('2099-12-31' AS DATE),
    {{ mapping.X_Hashkey }},
    [X_IsCurrent] = 1,
    [X_IsReplaced] = 0,
    [X_RunId] = '',
    [X_LoadDateTime] = GETDATE() AT TIME ZONE 'UTC' AT TIME ZONE 'W. Europe Standard Time',
    [X_Bron] = '{{mapping.DataSource}}'
    {% for sourceObject in mapping.SourceComposition %}
        {% if sourceObject.JoinType != 'APPLY' %}
            {{ sourceObject.JoinType }}
            [{{ sourceObject.Entity.CodeModel }}].[{{ sourceObject.Entity.Code }}] AS {{ sourceObject.JoinAlias }}
        {% endif %}
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
    {% endfor %}
WHERE
    1 = 1 {% for sourceObject in mapping.SourceComposition %}
        {% if sourceObject.JoinType == 'APPLY' and 'SourceConditions' in sourceObject %}
            AND {% for sourceCondition in sourceObject.SourceConditions %}
                {{ sourceCondition.SourceConditionVariable.SourceAttribute.EntityAlias }}.[{{ sourceCondition.SourceConditionVariable.SourceAttribute.Code }}]
            {% endfor %}

            {{ sourceObject.Entity.SqlExpression }}

            {% if 'JoinConditions' in sourceObject %}
                = {% for joinCondition in sourceObject.JoinConditions %}
                    {{ joinCondition.JoinConditionComponents.AttributeParent.EntityAlias }}.[{{ joinCondition.JoinConditionComponents.AttributeParent.Code }}]
                {% endfor %}
            {% endif %}
        {% endif %}
    {% endfor %}
