TRUNCATE TABLE [DA_MDDE].[LoadDependencies]
GO
INSERT INTO
    [DA_MDDE].[LoadDependencies] (
        [Model],
        [Mapping],
        [RelationType],
        [ModelRelated],
        [MappingRelated]
    ) {%- for dependency in mapping_dependencies %}
    SELECT
        '{{dependency.model}}',
        '{{dependency.name}}',
        '{{dependency.type_relation}}',
        '{{dependency.model_related}}',
        '{{dependency.name_related}}' {% if not loop.last %}
        UNION ALL
        {% endif %}
    {% endfor %}

    GO
