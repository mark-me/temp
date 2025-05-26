TRUNCATE TABLE [DA_MDDE].[Config]
GO

INSERT INTO [DA_MDDE].[Config] ([ModelName], [LayerName], [MappingName], [TargetName], [SourceName], [RunLevel], [RunLevelStage], [MaxTimestamp], [MaxTimestamp_LastRun], [LoadType], [LoadCommand], [LoadRunId], [LoadDateTime], [LoadOutcome])

{%- for mapping in config %}                                                              
SELECT  '{{mapping.NameModel}}', 
        '{{mapping.CodeModel}}', 
        '{{mapping.Name}}', 
        '{{mapping.TargetName}}', 
        '{{mapping.SourceViewName}}', 
        '{{mapping.RunLevel}}', 
        '{{mapping.RunLevelStage}}', 
        NULL, 
        NULL, 
        1, 
        'EXEC [DA_MDDE].[sp_LoadEntityData] ''00000000-0000-0000-0000-000000000000'',''{{mapping.CodeModel}}'',''{{mapping.SourceViewName}}'', ''{{mapping.TargetName}}'',''{{mapping.Name}}'' ,1 ,1', 
        NULL, 
        NULL, 
        NULL
{% if not loop.last %}UNION ALL {% endif %}{% endfor %}
GO 