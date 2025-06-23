CREATE VIEW [{{mapping.EntityTarget.CodeModel}}].[{{mapping.Name}}] AS
SELECT
    {% for identifier in mapping.Identifiers %}
        {{ identifier }},
    {% endfor %}

    {% for attributemapping in mapping.AttributeMapping %}
        {% if 'Expression' in attributemapping %}
            [{{ attributemapping.AttributeTarget.Code }}] = {{ attributemapping.Expression }},

            {% elif Expression not in attributemapping %}
            [{{attributemapping.AttributeTarget.Code}}] = {{ attributemapping.AttributesSource.EntityAlias }}.[{{attributemapping.AttributesSource.Code}}],
        {% endif %}
    {% endfor %}
    [X_StartDate] = CAST(GETDATE() AS DATE),
    [X_EndDate] = '2099-12-31',
    {{ mapping.X_Hashkey }},
    [X_IsCurrent] = 1,
    [X_IsReplaced] = 0,
    [X_RunId] = '',
    [X_LoadDateTime] = GETDATE(),
    [X_Bron] = '{{mapping.DataSource}}'
    {% for sourceObject in mapping.SourceComposition %}
        {% if sourceObject.JoinType != 'APPLY' %}
            {{ sourceObject.JoinType }}
            [{{ sourceObject.Entity.CodeModel }}].[{{ sourceObject.Entity.Name }}] AS {{ sourceObject.JoinAlias }}
        {% endif %}
        {% if 'JoinConditions' in sourceObject %}
            ON {% for joinCondition in sourceObject.JoinConditions %}
                {% if CodeModel|upper  == 'DA_CENTRAL' and joinCondition.ParentLiteral != '' %}
                    {{ sourceObject.JoinAlias }}.[{{ joinCondition.JoinConditionComponents.AttributeChild.Code }}] = {{ joinCondition.ParentLiteral }}
                {% elif CodeModel|upper   != 'DA_CENTRAL' and joinCondition.ParentLiteral != '' %}
                    {{ sourceObject.JoinAlias }}.[{{ joinCondition.JoinConditionComponents.AttributeChild.Name }}] = {{ joinCondition.ParentLiteral }}
                {% endif %}
                {% if joinCondition.ParentLiteral == '' and joinCondition.JoinConditionComponents.AttributeChild.CodeModel|upper   == 'DA_CENTRAL' %}
                    {{ sourceObject.JoinAlias }}.[{{ joinCondition.JoinConditionComponents.AttributeChild.Code }}] = {{ joinCondition.JoinConditionComponents.AttributeParent.EntityAlias }}.[{{ joinCondition.JoinConditionComponents.AttributeParent.Name }}]
                {% elif joinCondition.ParentLiteral == '' and joinCondition.JoinConditionComponents.AttributeChild.CodeModel|upper   != 'DA_CENTRAL' %}
                    {{ sourceObject.JoinAlias }}.[{{ joinCondition.JoinConditionComponents.AttributeChild.Name }}] = {{ joinCondition.JoinConditionComponents.AttributeParent.EntityAlias }}.[{{ joinCondition.JoinConditionComponents.AttributeParent.Code }}]
                {% endif %}
                {%- if not loop.last -%}
                    AND
                {% endif %}
            {% endfor %}
        {% endif %}
    {% endfor %}
WHERE
    1 = 1 {% for sourceObject in mapping.SourceComposition %}
        {% if sourceObject.JoinType == 'APPLY' %}
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
