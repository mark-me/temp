TRUNCATE TABLE [DA_MDDE].[Config]
GO

INSERT INTO [DA_MDDE].[Config]
           ([ModelName]
           ,[LayerName]
           ,[MappingName]
           ,[TargetName]
           ,[SourceName]
           ,[RunLevel]
           ,[RunLevelStage]
           ,[LoadTypeDefault]
           ,[LoadType]
           ,[LoadCommand]
           ,[LoadRunId]
           ,[LoadStartDateTime]
           ,[LoadEndDateTime]
           ,[LoadOutcome])
{%- for mapping in config %}                                                              
SELECT  '{{mapping.NameModel}}', 
        '{{mapping.CodeModel}}', 
        '{{mapping.MappingName}}', 
        '{{mapping.TargetName}}', 
        '{{mapping.SourceViewName}}', 
        '{{mapping.RunLevel}}', 
        '{{mapping.RunLevelStage}}', 
        1, 
        1, 
        'EXEC [DA_MDDE].[sp_LoadEntityData] ''00000000-0000-0000-0000-000000000000'',''{{mapping.CodeModel}}'',''{{mapping.SourceViewName}}'', ''{{mapping.TargetName}}'',''{{mapping.MappingName}}'' ,1 ,1', 
        NULL, 
        NULL, 
        NULL,
        NULL
{% if not loop.last %}UNION ALL {% endif %}{% endfor %}
GO 