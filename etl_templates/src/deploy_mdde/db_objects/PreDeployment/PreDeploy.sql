DECLARE @sql NVARCHAR(MAX)
PRINT ('****************************************************')
PRINT ('Running PreDeploy.sql.')
PRINT ('Remove unused tables and views in Da_Central schema.')
PRINT ('****************************************************')

IF OBJECT_ID('DA_MDDE.ConfigBase') IS NOT NULL
 SELECT @sql = string_agg(CAST(CONCAT (
 				'DROP '
 				, Soort
 				, ' ['
 				, SchemaNaam
 				, '].['
 				, NAME
 				, ']'
 				) AS NVARCHAR(MAX)), '; ' + CHAR(13))
 FROM (
 	SELECT 'TABLE' AS Soort
 		, NAME
 		, schema_name(schema_id) AS SchemaNaam
 	FROM sys.tables
 	WHERE 1 = 1 AND NAME NOT IN (
 			SELECT [Destination]
 			FROM [DA_MDDE].[ConfigBase]
 			) AND schema_name(schema_id) = 'Da_Central' AND NAME NOT IN ('Calendar')
 	
 	UNION ALL
 	
 	SELECT 'VIEW' AS Soort
 		, NAME
 		, schema_name(schema_id) AS SchemaNaam
 	FROM sys.VIEWS
 	WHERE 1 = 1 AND NAME NOT IN (
 			SELECT [Source]
 			FROM [DA_MDDE].[ConfigBase]
 			) AND schema_name(schema_id) = 'Da_Central' AND NAME NOT IN ('vw_CalendarHolidays', 'vw_src_Calendar')
 	) A

PRINT (@sql)
--PRINT ('PreDeploy execution is disabled in PreDeploy.sql.')
EXEC sp_executesql @sql
