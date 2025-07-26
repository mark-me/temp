TRUNCATE TABLE [DA_MDDE].[LoadDependencies]
GO
INSERT INTO
    [DA_MDDE].[LoadDependencies] (
        [Model],
        [Mapping],
        [PrecedingModel],
        [PrecedingMapping]
    ) {%- for dependency in mapping_dependencies %}
    SELECT
        '{{dependency.model}}',
        '{{dependency.name}}',
        '{{dependency.model_preceding}}',
        '{{dependency.mapping_preceding}}' {% if not loop.last %}
        UNION ALL
        {% endif %}
    {% endfor %}

    GO
