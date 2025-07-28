TRUNCATE TABLE [DA_MDDE].[ConfigMappingClusters]
GO
INSERT INTO
    [DA_MDDE].[ConfigMappingClusters] (
        [Schema],
        [Mapping],
        [Cluster]
    ) {%- for mapping_cluster in mapping_clusters %}
    SELECT
        '{{mapping_cluster.CodeModel}}',
        '{{mapping_cluster.Mapping}}',
        '{{mapping_cluster.Cluster}}' {% if not loop.last %}
        UNION ALL
        {% endif %}
    {% endfor %}

    GO
