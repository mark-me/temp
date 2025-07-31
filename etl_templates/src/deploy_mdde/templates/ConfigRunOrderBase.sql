TRUNCATE TABLE [DA_MDDE].[ConfigBase]
GO

INSERT INTO [DA_MDDE].[ConfigBase]
           ([Model]
           ,[Schema]
           ,[Mapping]
           ,[Source]
           ,[Destination]
           ,[RunLevel]
           ,[RunLevelStage]
           ,[LoadType]
          )
{%- for mapping in config %}                                                              
SELECT  '{{mapping.NameModel}}', 
        '{{mapping.CodeModel}}', 
        '{{mapping.MappingName}}', 
        '{{mapping.SourceViewName}}', 
        '{{mapping.TargetName}}', 
        {{mapping.RunLevel}}, 
        {{mapping.RunLevelStage}}, 
        {% if mapping.CodeModel|upper  == 'DA_Central_Staging' %}
        0
        {% elif mapping.CodeModel|upper  == 'DA_CENTRAL' and mapping.TargetName[:3]|upper == 'AGG' %}
        0
        {% elif mapping.CodeModel|upper  == 'DA_CENTRAL' %}
        1
        {% elif mapping.CodeModel|upper  == 'DM_Dim' %}
        0
        {% elif mapping.CodeModel|upper  == 'DM_Fact' %}
        0
        {% else %}
        0
        {% endif %}
{% if not loop.last %}UNION ALL {% endif %}{% endfor %}
GO 