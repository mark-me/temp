CREATE PROC [DA_MDDE].[sp_TruncateFullLoadDestination] @par_runid [NVARCHAR] (500)
AS
/***************************************************************************************************
Script Name         sp_TruncateFullLoadDestination.sql
Create Date:        2025-06-16
Author:             Jeroen Poll
Description:        PROC
Used By:            -
Parameter(s):       - 	  @par_runid [NVARCHAR] (500)			= ETL RunID form the Synapse Pipeline. 
						
Usage:              -
****************************************************************************************************
SUMMARY OF CHANGES
Date(yyyy-mm-dd)    Author              Comments
------------------- ------------------- ------------------------------------------------------------
2025-06-16	        Jeroen Poll         Initial Script V1.0  
2025-07-01			Jeroen Poll			Optimize script to one statement.

***************************************************************************************************/
DECLARE @LogMessage NVARCHAR(max);
DECLARE @sql NVARCHAR(MAX);

BEGIN
SELECT @sql = string_agg(CAST(CONCAT (
				'TRUNCATE '
				, Soort
				, ' ['
				, SchemaNaam
				, '].['
				, NAME
				, ']'
				) AS NVARCHAR(MAX)), '; ' + CHAR(13))
FROM (
	SELECT 'TABLE' AS Soort
		, s.[NAME]
		, schema_name(s.[schema_id]) AS SchemaNaam
	FROM sys.tables s
	INNER JOIN [DA_MDDE].[ConfigExecution] c ON c.[LoadRunId]= @par_runid AND c.[Schema] = schema_name(s.[schema_id]) AND c.[Destination] = s.[NAME] AND c.[LoadType] IN( 0, 90) 

	GROUP BY s.[NAME]
		, schema_name(s.[schema_id])
	) A

PRINT '    --------TRUNCATE TABLES SCRIPT--------'
--PRINT (@sql)
EXEC sp_executesql @sql
END
GO