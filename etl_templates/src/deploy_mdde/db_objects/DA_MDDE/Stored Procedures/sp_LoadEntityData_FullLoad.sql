CREATE PROC [DA_MDDE].[sp_LoadEntityData_FullLoad] @par_runid [NVARCHAR](500),@par_LayerName [NVARCHAR](500),@par_SourceName [NVARCHAR](500),@par_DestinationName [NVARCHAR](500),@par_MappingName [NVARCHAR](500),@par_Debug [Bit] AS
/***************************************************************************************************
Script Name         sp_LoadEntityData_FullLoad.sql
Create Date:        2025-02-17
Author:             Jeroen Poll
Description:        PROC
Used By:            -
Parameter(s):       - @par_runid [NVARCHAR] (500)  /* ETL RunID form the Synapse Pipeline. */
					, @par_LayerName [NVARCHAR] (500)  /* Schema Name. */
					, @par_SourceName [NVARCHAR] (500) /* Source table or view Name. */
					, @par_DestinationName [NVARCHAR] (500) /* Desitination table or view Name. */
					, @par_MappingName [NVARCHAR] (500) /* Mapping name from PowerDesigner. Is only used for logging. */
					, @par_Debug [Boolean] /* If true, only statement will be printed and not executed. */

Usage:              -
****************************************************************************************************
SUMMARY OF CHANGES
Date(yyyy-mm-dd)    Author              Comments
------------------- ------------------- ------------------------------------------------------------
2025-02-17	        Jeroen Poll         Initial Script V1.0   First version Full Load Only
2025-04-04			Jeroen Poll			Added real column names and not select *
2025-04-18			Avinash Kalicharan	Add debug to the procedure
2025-05-20			Jeroen Poll			Add Config Execution proc
2025-06-16			Jeroen Poll			Remove Truncate, Will be done in other PROC
2025-06-19			Jeroen Poll			Fix param Runid id not correct insert statement.
2025-07-03			Jeroen Poll			Add support for Loading with constraint info
***************************************************************************************************/
BEGIN TRY
	SET NOCOUNT ON;
	DECLARE @sql NVARCHAR(MAX) = ''
	DECLARE @sql_select NVARCHAR(MAX) = ''
	DECLARE @sql_insert NVARCHAR(MAX) = ''
	DECLARE @sql_filter NVARCHAR(MAX) = ''
	DECLARE @LoadType_UniqueKey INT
	DECLARE @LoadType_BKey BIT
	DECLARE @LogMessage NVARCHAR(MAX);
	DECLARE @ExecutionId UNIQUEIDENTIFIER = newid()
	DECLARE @sqlNewRow NVARCHAR(50) = CHAR(13) + CHAR(10)
	DECLARE @sqlRowcount NVARCHAR(MAX) = ''
	DECLARE @rowcount_New BIGINT
	DECLARE @rowcount_Update BIGINT
	DECLARE @sel NVARCHAR(max)
	DECLARE @ErrorMessage NVARCHAR(4000);
	DECLARE @ErrorSeverity INT;
	DECLARE @ErrorState INT;


	/* Update Config_Start in config table */
	SET @LogMessage = CONCAT(@par_runid,'¡','Update Config table with StartDateTime')
	EXEC [DA_MDDE].[sp_Logger] 'INFO', @LogMessage
	EXEC [DA_MDDE].[sp_UpdateConfig_Start] @par_runid , @par_LayerName , @par_MappingName 	, @par_Debug 

	IF (@par_Debug = 1)
		BEGIN 
			SET @LogMessage = CONCAT(@par_runid,'¡','Debug is set to True')
			EXEC [DA_MDDE].[sp_Logger] 'INFO', @LogMessage
		END

	/* Check if table has Unique Key constraint, needed for loading the data */
	SELECT @LoadType_UniqueKey = COUNT(*)
	FROM information_schema.TABLE_CONSTRAINTS c
	WHERE 1 = 1 AND c.TABLE_SCHEMA = @par_LayerName AND c.TABLE_NAME = @par_DestinationName AND c.CONSTRAINT_TYPE = 'UNIQUE'

	/* Check if table has BKey for loading */
	SELECT @LoadType_BKey = CASE 
			WHEN COUNT(*) > 0
				THEN 1
			ELSE 0
			END
	FROM information_schema.TABLES t
	INNER JOIN information_schema.COLUMNS c ON t.TABLE_SCHEMA = c.TABLE_SCHEMA AND t.TABLE_NAME = c.TABLE_NAME
	WHERE 1 = 1 AND t.TABLE_SCHEMA = @par_LayerName AND t.TABLE_NAME = @par_DestinationName AND c.COLUMN_NAME = @par_DestinationName + 'BKey'

	/* 
	Build Dynamic SQL statement: Insert INTO
	For loading data, you need to define the table and the columns you want to load.
	This code builds up the INSERT INTO part of this statement.

*/
	SELECT @sql_insert = sqlcode
	FROM (
		SELECT CONCAT (
				'INSERT INTO [', @par_LayerName, '].[', @par_DestinationName, ']', CHAR(13), CHAR(10), '(', CHAR(13), CHAR(10), STRING_AGG(CHAR(9) + '[' + dest.[name] + ']', ', ' + CHAR(13) + CHAR(10)) WITHIN GROUP (
						ORDER BY dest.column_id ASC
						), CHAR(13), CHAR(10), ')', CHAR(13), CHAR(10)
				) AS sqlcode
		FROM sys.columns dest
		LEFT JOIN sys.columns source ON source.object_id = OBJECT_ID(CONCAT (QUOTENAME(@par_LayerName), '.', QUOTENAME(@par_SourceName))) AND dest.[name] = source.[name]
		WHERE dest.object_id = OBJECT_ID(CONCAT (QUOTENAME(@par_LayerName), '.', QUOTENAME(@par_DestinationName))) AND dest.is_identity = 0
		) a;

	/* 
	Build Dynamic SQL statement: SELECT
	For loading data, you need to what data you want to load in to the table.
	This code builds up the a dynamic select statement without filters.

*/
	SELECT @sql_select = sqlcode
	FROM (
		SELECT CONCAT (
				'SELECT ', CHAR(13), CHAR(10), STRING_AGG(CONCAT (
						CHAR(9), '[' + dest.[name] + ']', ' = ', CASE 
							WHEN source.[name] = 'X_RunId'
								THEN '''' + @par_runid + ''''
							ELSE 'source.' + '[' + source.[name] + ']'
							END
						), ', ' + CHAR(13) + CHAR(10)) WITHIN GROUP (
						ORDER BY dest.column_id ASC
						), CHAR(13), CHAR(10), CONCAT ('FROM [', @par_LayerName, '].[', @par_SourceName, '] as source', CHAR(13), CHAR(10)), 'WHERE 1=1', CHAR(13), CHAR(10)
				) AS sqlcode
		FROM sys.columns dest
		LEFT JOIN sys.columns source ON source.object_id = OBJECT_ID(CONCAT (QUOTENAME(@par_LayerName), '.', QUOTENAME(@par_SourceName))) AND dest.[name] = source.[name]
		WHERE dest.object_id = OBJECT_ID(CONCAT (QUOTENAME(@par_LayerName), '.', QUOTENAME(@par_DestinationName))) AND dest.is_identity = 0
		) a;

	/* 
	Build Dynamic SQL statement: Insert INTO
	For loading data, you need to define the table and the columns you want to load.
	This code builds up the INSERT INTO part of this statement.

*/
	IF (@LoadType_UniqueKey > 1)
	BEGIN
		SET @ErrorMessage = CONCAT (N'ERROR: More then 1 "UNIQUE" Key active on table :', QUOTENAME(@par_LayerName), '.', QUOTENAME(@par_DestinationName), CHAR(13), 'Use Query to show Keys: ', CHAR(13), 'SELECT * FROM information_schema.TABLE_CONSTRAINTS WHERE TABLE_SCHEMA = ''', @par_LayerName, ''' AND TABLE_NAME = ''', @par_DestinationName, '''')

		RAISERROR (
				@ErrorMessage,
				-- Message text.
				16, -- Severity.
				1 -- State.
				);
	END
	ELSE IF @LoadType_UniqueKey = 1
	BEGIN
		SELECT @sql_filter = sqlcode
		FROM (
			SELECT CONCAT (
					'AND NOT EXISTS (SELECT 1 FROM [', @par_LayerName, '].[', @par_DestinationName, '] AS destination WHERE ', STRING_AGG(CONCAT ('destination.[', cc.COLUMN_NAME, '] = source.[', cc.COLUMN_NAME, ']'), ' AND ') WITHIN GROUP (
							ORDER BY cc.COLUMN_NAME ASC
							), ')'
					) AS sqlcode
			FROM information_schema.TABLE_CONSTRAINTS c
			INNER JOIN information_schema.CONSTRAINT_COLUMN_USAGE cc ON c.TABLE_SCHEMA = cc.TABLE_SCHEMA AND c.TABLE_NAME = cc.TABLE_NAME AND c.CONSTRAINT_NAME = cc.CONSTRAINT_NAME
			WHERE 1 = 1 AND c.TABLE_SCHEMA = @par_LayerName AND c.TABLE_NAME = @par_DestinationName AND c.CONSTRAINT_TYPE = 'UNIQUE'
			) a;
	END
	ELSE IF @LoadType_BKey = 1
	BEGIN
		SELECT @sql_filter = sqlcode
		FROM (
			SELECT CONCAT (
					N'', CASE 
						WHEN LOWER(left(@par_DestinationName, 4)) = 'aggr'
							THEN N''
						ELSE CONCAT ('AND NOT EXISTS (SELECT 1 FROM [', @par_LayerName, '].[', @par_DestinationName, '] AS destination WHERE destination.[', @par_DestinationName, 'BKey] =  source.[', @par_DestinationName, 'BKey])')
						END
					) AS sqlcode
			) a;
	END
	ELSE
		SET @sql_filter = N''

	SET @sqlRowcount = (
			SELECT CONCAT ('select @outputFromExec = count(*) FROM (', @sql_select, @sql_filter, ') a')
			)
	SET @sql = CONCAT (@sql_insert, CHAR(13), @sql_select, CHAR(13), @sql_filter, CHAR(13))

	IF (@par_Debug = 1)
		BEGIN
			SET @logmessage = CONCAT (@par_runid, '¡', @sqlRowcount)

			EXEC [DA_MDDE].[sp_Logger] 'INFO', @logmessage
		END
	ELSE
		BEGIN
			EXEC [DA_MDDE].[sp_Logger] 'INFO', N'Run Query to get rowcount for new insert.'
			EXEC sp_executesql @sqlRowcount, N'@outputFromExec bigint out', @rowcount_New OUT
		END

	
	/* Update rowcount in Config table */
	EXEC [DA_MDDE].[sp_Logger] 'INFO', N'Update Config table with Rowcounts.'
	EXEC [DA_MDDE].[sp_UpdateConfig_RowCount]  @par_runid , @par_LayerName , @par_MappingName , @rowcount_New, 0, 0	, @par_Debug 
	SET @LogMessage = CONCAT (@par_runid, '¡', 'Rowcount to be inserted into ', '[', @par_LayerName, '].[', @par_DestinationName, ']', ' is: ', @rowcount_New)
	EXEC [DA_MDDE].[sp_Logger] 'INFO', @LogMessage

	IF (@par_Debug = 1)
	BEGIN
		EXEC [DA_MDDE].[sp_Logger] 'INFO', @sql
	END
	ELSE
	BEGIN
		EXEC [DA_MDDE].[sp_Logger] 'INFO', N'Run Query for new insert.'
		EXEC sp_executesql @sql
	END
		
	/* Update config table with OK and timestamp */
	EXEC [DA_MDDE].[sp_Logger] 'INFO', N'Update Config table with sp_UpdateConfig_End.'
	EXEC [DA_MDDE].[sp_UpdateConfig_End]  @par_runid , @par_LayerName , @par_MappingName , @par_Debug 

END TRY

BEGIN CATCH
    SELECT @ErrorMessage = ERROR_MESSAGE(),
           @ErrorSeverity = ERROR_SEVERITY(),
           @ErrorState = ERROR_STATE();

	SELECT ERROR_NUMBER() AS ErrorNumber, ERROR_MESSAGE() AS ErrorMessage;
	/* Update config table with NOK, timestamp and run sp_UpdateConfig_ErrorPredecessor to update predecessors */
	SET @LogMessage = CONCAT (@par_runid,'¡',N'Update Config Table with uitcome NOK')
	EXEC [DA_MDDE].[sp_Logger] 'INFO', @LogMessage
	EXEC [DA_MDDE].[sp_UpdateConfig_Error]  @par_runid , @par_LayerName , @par_MappingName , @par_Debug 

	SET @LogMessage = CONCAT (@par_runid,'¡','Error loding folowing command:')
	EXEC [DA_MDDE].[sp_Logger] 'ERROR', @LogMessage
	SET @LogMessage = CONCAT (@par_runid,'¡','Error Message: ', @ErrorMessage)
	EXEC [DA_MDDE].[sp_Logger] 'ERROR', @LogMessage
    RAISERROR(@ErrorMessage, @ErrorSeverity, @ErrorState);
END CATCH
GO

