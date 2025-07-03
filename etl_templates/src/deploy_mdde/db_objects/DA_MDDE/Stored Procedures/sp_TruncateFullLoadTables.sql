CREATE PROC [DA_MDDE].[sp_TruncateFullLoadTables] @par_runid [NVARCHAR] (500)
AS
/***************************************************************************************************
Script Name         sp_TruncateFullLoadTables.sql
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

***************************************************************************************************/
DECLARE @LogMessage NVARCHAR(max);

BEGIN
	SET @LogMessage = CONCAT (
			@par_runid
			, '¡'
			, 'Begin truncate van de Full Load Tabellen. '
			)

	EXEC [DA_MDDE].[sp_Logger] 'INFO'
		, @LogMessage
END

IF OBJECT_ID('tempdb..#TablesToBeTruncatedFL') IS NOT NULL
BEGIN
	DROP TABLE #TablesToBeTruncatedFL
END

CREATE TABLE #TablesToBeTruncatedFL (
	Id INT IDENTITY(1, 1)
	, TableObjectId INT
	, SchemaName SYSNAME
	, TableName SYSNAME
	, SchemaId INT
	)

INSERT INTO #TablesToBeTruncatedFL (
	TableObjectId
	, SchemaName
	, TableName
	, SchemaId
	)
SELECT ST.object_id
	, schema_name(ST.schema_id)
	, ST.name
	, ST.schema_id
FROM [DA_MDDE].[Config] C
INNER JOIN sys.Tables ST ON C.[TargetName] = ST.name AND C.[LayerName] = schema_name(ST.schema_id)
WHERE 1 = 1 AND [LoadType] <> 99 AND LoadType = 0
GROUP BY ST.object_id
	, schema_name(ST.schema_id)
	, ST.name
	, ST.schema_id

PRINT 'BEGIN TRANSACTION TruncateTables'
PRINT 'BEGIN TRY'
PRINT '    --------TRUNCATE TABLES SCRIPT--------'

DECLARE @id INT
	, @truncatescript NVARCHAR(MAX)

SELECT @id = MIN(Id)
FROM #TablesToBeTruncatedFL

WHILE @id IS NOT NULL
BEGIN
	SELECT @truncatescript = '    TRUNCATE TABLE ' + QUOTENAME(SchemaName) + '.' + QUOTENAME(TableName)
	FROM #TablesToBeTruncatedFL
	WHERE Id = @id

	SET @LogMessage = CONCAT (
			@par_runid
			, '¡'
			, @truncatescript
			)

	EXEC [DA_MDDE].[sp_Logger] 'INFO'
		, @LogMessage

	EXEC sp_executesql @truncatescript;

	SELECT @id = MIN(Id)
	FROM #TablesToBeTruncatedFL
	WHERE Id > @id
END
GO