DELETE FROM [DA_MDDE].[CodeList]
GO

INSERT INTO [DA_MDDE].[CodeList] ([SourceSystem], [ElementName], [Code], [LabelEN], [DescriptionEN], [LabelNL], [DescriptionNL], [X_StartDate], [X_EndDate], [X_HashKey], [X_IsCurrent], [X_IsReplaced], [X_RunId], [X_LoadDateTime], [X_Bron])

{%- for cl in codeList %}                                                              
SELECT  '{{cl.SourceSystem}}' AS [SourceSystem],
        '{{cl.ElementName}}' AS [ElementName], 
        '{{cl.Code.replace("'", "''")}}' AS [Code], 
        '{{cl.Label_EN.replace("'", "''")}}' AS [LabelEN], 
        '{{cl.Description_EN.replace("'", "''")}}' AS [DescriptionEN], 
        '{{cl.Label_NL.replace("'", "''")}}' AS [LabelNL], 
        '{{cl.Description_NL.replace("'", "''")}}' AS [DescriptionNL], 
        GETDATE() AS [X_StartDate], 
        CAST('2099-12-31' as date) AS [X_EndDate], 
        HASHBYTES('SHA2_512', CONCAT ('{{cl.SourceSystem}}' , '{{cl.ElementName}}', '{{cl.Code}}' )) AS [X_HashKey], 
        CAST(1 as bit) AS [X_IsCurrent], 
        CAST(0 as bit) AS [X_IsReplaced], 
        'N/A' AS [X_RunId],
	GETDATE() AS [X_LoadDateTime],
	'{{cl.SourceSystem}}' AS [X_Bron]
{% if not loop.last %}UNION ALL {% endif %}{% endfor %}
GO 