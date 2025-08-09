CREATE PROC [DA_MDDE].[sp_LoadEntity_Incremental] @par_runid [NVARCHAR](500),@par_Schema [NVARCHAR](500),@par_Source [NVARCHAR](500),@par_Destination [NVARCHAR](500),@par_Mapping [NVARCHAR](500),@par_Debug [Bit] AS
/***************************************************************************************************
Script Name         sp_LoadEntity_Incremental.sql
Create Date:        2025-02-17
Author:             Jeroen Poll
Description:        PROC
Used By:            -
Parameter(s):       - @par_runid [NVARCHAR] (500)  /* ETL RunID form the Synapse Pipeline. */
					, @par_Schema [NVARCHAR] (500)  /* Schema Name. */
					, @par_Source [NVARCHAR] (500) /* Source table or view Name. */
					, @par_Destination [NVARCHAR] (500) /* Desitination table or view Name. */
					, @par_Mapping [NVARCHAR] (500) /* Mapping name from PowerDesigner. Is only used for logging.  */
Usage:              -
****************************************************************************************************
SUMMARY OF CHANGES
Date(yyyy-mm-dd)    Author              Comments
------------------- ------------------- ------------------------------------------------------------
2025-02-17	        Jeroen Poll         Initial Script V1.0   First version Delta Load
2025-04-04			Jeroen Poll			Add column names.
2025-04-22			Avinash Kalicharan	Change SP name to 'Incremental'
2025-04-22			Avinash Kalicharan 	Add debug to the procedure
2025-05-22			Jeroen Poll			Add brackets to names and raise error in CATCH
2025-06-19			Jeroen Poll			Fix param Runid id not correct insert statement.
2025-07-21			Jeroen Poll			Added Code for loading with new key (Unique Key constraints)
2025-08-05			Youri Jongejan/		Updated logging
					Avinash Kalicharan	
***************************************************************************************************/
BEGIN TRY
	SET NOCOUNT ON;
	DECLARE @sql NVARCHAR(MAX) = ''
	DECLARE @sql_select NVARCHAR(MAX) = ''
	DECLARE @sql_insert NVARCHAR(MAX) = ''
	DECLARE @sql_insertnew NVARCHAR(MAX) = ''
	DECLARE @sql_filter NVARCHAR(MAX) = ''
	DECLARE @sql_filter2 NVARCHAR(MAX) = ''
	DECLARE @sql_filter3 NVARCHAR(MAX) = ''
	DECLARE @sql_update NVARCHAR(MAX) = ''
	DECLARE @sql_updateExisting NVARCHAR(MAX) = ''
	DECLARE @LoadType_UniqueKey INT
	DECLARE @LoadType_BKey BIT
	DECLARE @LogMessage NVARCHAR(MAX);
	DECLARE @ExecutionId UNIQUEIDENTIFIER = newid()
	DECLARE @sqlNewRow NVARCHAR(50) = CHAR(13) + CHAR(10)
	DECLARE @sqlRowcount NVARCHAR(MAX) = ''
	DECLARE @RowCountInsert BIGINT
	DECLARE @RowCountUpdate BIGINT
	DECLARE @RowCountDelete BIGINT = 0
	DECLARE @sel NVARCHAR(max)
	DECLARE @ErrorMessage NVARCHAR(4000);
	DECLARE @ErrorSeverity INT;
	DECLARE @ErrorState INT;

	IF (@par_Debug = 1)
		BEGIN 
			EXEC [DA_MDDE].[sp_Logger] 'INFO', 'Debug is set to True'
		END
	ELSE
		BEGIN
			EXEC sp_executesql @sql
		END
	/* Check if table has Unique Key for loading */
	SELECT @LoadType_UniqueKey = COUNT(*)
	FROM information_schema.TABLE_CONSTRAINTS c
	WHERE 1 = 1 AND c.TABLE_SCHEMA = @par_Schema AND c.TABLE_NAME = @par_Destination AND c.CONSTRAINT_TYPE = 'UNIQUE'

	/* Check if table has BKey for loading */
	SELECT @LoadType_BKey = CASE 
			WHEN COUNT(*) > 0
				THEN 1
			ELSE 0
			END
	FROM information_schema.TABLES t
	INNER JOIN information_schema.COLUMNS c ON t.TABLE_SCHEMA = c.TABLE_SCHEMA AND t.TABLE_NAME = c.TABLE_NAME
	WHERE 1 = 1 AND t.TABLE_SCHEMA = @par_Schema AND t.TABLE_NAME = @par_Destination AND c.COLUMN_NAME = @par_Destination + 'BKey'

	/* 
		Build Dynamic SQL statement: Insert INTO
		For loading data, you need to define the table and the columns you want to load.
		This code builds up the INSERT INTO part of this statement.

	*/
	SELECT @sql_insert = sqlcode
	FROM (
		SELECT CONCAT (
				'INSERT INTO [', @par_Schema, '].[', @par_Destination, ']', CHAR(13), CHAR(10), '(', CHAR(13), CHAR(10), STRING_AGG(CHAR(9) + '[' + dest.[name] + ']', ', ' + CHAR(13) + CHAR(10)) WITHIN GROUP (
						ORDER BY dest.column_id ASC
						), CHAR(13), CHAR(10), ')', CHAR(13), CHAR(10)
				) AS sqlcode
		FROM sys.columns dest
		LEFT JOIN sys.columns source ON source.object_id = OBJECT_ID(CONCAT (QUOTENAME(@par_Schema), '.', QUOTENAME(@par_Source))) AND dest.[name] = source.[name]
		WHERE dest.object_id = OBJECT_ID(CONCAT (QUOTENAME(@par_Schema), '.', QUOTENAME(@par_Destination))) AND dest.is_identity = 0
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
						), CHAR(13), CHAR(10), CONCAT ('FROM [', @par_Schema, '].[', @par_Source, '] as source', CHAR(13), CHAR(10)), 'WHERE 1=1', CHAR(13), CHAR(10)
				) AS sqlcode
		FROM sys.columns dest
		LEFT JOIN sys.columns source ON source.object_id = OBJECT_ID(CONCAT (QUOTENAME(@par_Schema), '.', QUOTENAME(@par_Source))) AND dest.[name] = source.[name]
		WHERE dest.object_id = OBJECT_ID(CONCAT (QUOTENAME(@par_Schema), '.', QUOTENAME(@par_Destination))) AND dest.is_identity = 0
		) a;

	/* 
		Build Dynamic SQL statement: Insert INTO
		For loading data, you need to define the table and the columns you want to load.
		This code builds up the INSERT INTO part of this statement.

	*/
	IF (@LoadType_UniqueKey > 1)
	BEGIN
		SET @ErrorMessage = CONCAT (N'ERROR: More then 1 "UNIQUE" Key active on table :', QUOTENAME(@par_Schema), '.', QUOTENAME(@par_Destination), CHAR(13), 'Use Query to show Keys: ', CHAR(13), 'SELECT * FROM information_schema.TABLE_CONSTRAINTS WHERE TABLE_SCHEMA = ''', @par_Schema, ''' AND TABLE_NAME = ''', @par_Destination, '''')

		RAISERROR (
				@ErrorMessage,
				-- Message text.
				16,
				-- Severity.
				1 -- State.
				);
	END
	ELSE IF @LoadType_UniqueKey = 1
	BEGIN
		SELECT @sql_filter = sqlcode, @sql_filter2 = sqlcode2, @sql_filter3 = sqlcode3
		FROM (
			SELECT CONCAT (
					'AND NOT EXISTS (SELECT 1 FROM [', @par_Schema, '].[', @par_Destination, '] AS destination WHERE ', STRING_AGG(CONCAT ('destination.[', cc.COLUMN_NAME, '] = source.[', cc.COLUMN_NAME, ']'), ' AND ') WITHIN GROUP (
							ORDER BY cc.COLUMN_NAME ASC
							), ')'
					) AS sqlcode
					,CONCAT (
					'AND EXISTS (SELECT 1 FROM [', @par_Schema, '].[', @par_Destination, '] AS destination WHERE ', STRING_AGG(CONCAT ('destination.[', cc.COLUMN_NAME, '] = source.[', cc.COLUMN_NAME, ']'), ' AND ') WITHIN GROUP (
							ORDER BY cc.COLUMN_NAME ASC
							), ' AND source.[X_HashKey] <> destination.[X_HashKey] AND destination.[X_IsCurrent] = 1)'
					) AS sqlcode2
					,CONCAT (
					N' AND ', STRING_AGG(CONCAT ('destination.[', cc.COLUMN_NAME, '] = source.[', cc.COLUMN_NAME, ']'), ' AND ') WITHIN GROUP (
							ORDER BY cc.COLUMN_NAME ASC
							)
					) AS sqlcode3
			FROM information_schema.TABLE_CONSTRAINTS c
			INNER JOIN information_schema.CONSTRAINT_COLUMN_USAGE cc ON c.TABLE_SCHEMA = cc.TABLE_SCHEMA AND c.TABLE_NAME = cc.TABLE_NAME AND c.CONSTRAINT_NAME = cc.CONSTRAINT_NAME
			WHERE 1 = 1 AND c.TABLE_SCHEMA = @par_Schema AND c.TABLE_NAME = @par_Destination AND c.CONSTRAINT_TYPE = 'UNIQUE'
			) a;
	END
	ELSE IF @LoadType_BKey = 1
	BEGIN
		SELECT  @sql_filter = sqlcode, @sql_filter2 = sqlcode2, @sql_filter3 = sqlcode3
		FROM (
			SELECT CONCAT (
					N'', CASE 
						WHEN LOWER(left(@par_Destination, 4)) = 'aggr'
							THEN N''
						ELSE CONCAT ('AND NOT EXISTS (SELECT 1 FROM [', @par_Schema, '].[', @par_Destination, '] AS destination WHERE destination.[', @par_Destination, 'BKey] =  source.[', @par_Destination, 'BKey] AND destination.[X_IsCurrent] = 1)')
						END
					) AS sqlcode
					,CONCAT (
					N'', CASE 
						WHEN LOWER(left(@par_Destination, 4)) = 'aggr'
							THEN N''
						ELSE CONCAT ('AND EXISTS (SELECT 1 FROM [', @par_Schema, '].[', @par_Destination, '] AS destination WHERE destination.[', @par_Destination, 'BKey] =  source.[', @par_Destination, 'BKey]  AND source.[X_HashKey] <> destination.[X_HashKey] AND destination.[X_IsCurrent] = 1)')
						END
					) AS sqlcode2
					,CONCAT (
					N'', CASE 
						WHEN LOWER(left(@par_Destination, 4)) = 'aggr'
							THEN N''
						ELSE CONCAT ('AND destination.[', @par_Destination, 'BKey] =  source.[', @par_Destination, 'BKey] ')
						END
					) AS sqlcode3
			) a;
	END
	ELSE
		SET @sql_filter = N''

	/* Build Dynamic SQL statement: Update existing records with defferent X_HashKey */
	SET @sql_updateExisting = CONCAT ('UPDATE ', '[', @par_Schema, '].[', @par_Destination, ']', CHAR(13), CHAR(9), 'SET [X_EndDate] = getdate()-1', CHAR(13), CHAR(9), CHAR(9), ',[X_IsCurrent] = 0', CHAR(13), CHAR(9), CHAR(9), ',[X_IsReplaced] = 0', CHAR(13), 'FROM ', '[', @par_Schema, '].[', @par_Source, ']', ' AS source', CHAR(13), 'INNER JOIN ', '[', @par_Schema, '].[', @par_Destination, ']', ' AS destination', CHAR(13), CHAR(9), 'ON source.[X_HashKey] <> destination.[X_HashKey] AND destination.[X_IsCurrent] = 1 ', CHAR(13), CHAR(9), @sql_filter3)
	/* Insert New Rows (Unique Key does noet exist in destination) */
	SET @sql_insertnew = CONCAT (@sql_insert, CHAR(13), @sql_select, @sql_filter, CHAR(13))
	/* Insert Updated Rows (Unique Key does exist in destination, with different X_HashKey) */
	SET @sql_update = CONCAT (@sql_insert, CHAR(13), @sql_select, CHAR(13), @sql_filter2, CHAR(13))

	/* Get RowCount from queries */
	BEGIN
		SET @sqlRowcount = (SELECT CONCAT ('select @outputFromExec = count(*) FROM (', @sql_select, @sql_filter, ') a'))
		SET @LogMessage = CONCAT(@par_runid,'¡', N'Run Query to get rowcount for new insert.')
		EXEC [DA_MDDE].[sp_Logger] 'INFO', @LogMessage

	END
	IF (@par_Debug <> 1)
		BEGIN
			EXEC sp_executesql @sqlRowcount, N'@outputFromExec bigint out', @RowCountInsert OUT
			SET @LogMessage = CONCAT(@par_runid,'¡', N'RowCount for new records is: ' , @RowCountInsert)
			EXEC [DA_MDDE].[sp_Logger] 'INFO', @LogMessage
		END
	BEGIN
		SET @sqlRowcount = (SELECT CONCAT ('select @outputFromExec = count(*) FROM (', @sql_select, @sql_filter2, ') a'))
		SET @LogMessage = CONCAT(@par_runid,'¡', N'Run Query to get rowcount for Updated records.')
		EXEC [DA_MDDE].[sp_Logger] 'INFO', @LogMessage
	END
	IF (@par_Debug <> 1)
		BEGIN
			EXEC sp_executesql @sqlRowcount, N'@outputFromExec bigint out', @RowCountUpdate OUT
			SET @LogMessage = CONCAT(@par_runid,'¡', N'RowCount for Updated records is: ' , @RowCountUpdate)
			EXEC [DA_MDDE].[sp_Logger] 'INFO', @LogMessage
		END

	/* Update rowcount in Config table */
	SET @LogMessage = CONCAT(@par_runid,'¡',N'Update Config table with Rowcounts.')
	EXEC [DA_MDDE].[sp_Logger] 'INFO', @LogMessage
	EXEC [DA_MDDE].[sp_UpdateConfig_RowCount]  @par_runid , @par_Schema , @par_Mapping , @RowCountInsert, @RowCountUpdate, 0	, @par_Debug 
	EXEC [DA_MDDE].[sp_EndEntity_Execution] @par_runid,@par_schema ,@par_mapping , @RowCountInsert , @RowCountUpdate, @RowCountDelete
	/*Run Queries to load data*/
	BEGIN
		EXEC [DA_MDDE].[sp_Logger] 'INFO', N'Run Query to load for new records:'
		PRINT ('********************')
		PRINT (@sql_insertnew)
		PRINT ('********************')
	END
	IF (@par_Debug <> 1)
		EXEC sp_executesql @sql_insertnew 
	BEGIN
		EXEC [DA_MDDE].[sp_Logger] 'INFO', N'Run Query to update changed records:'
		PRINT ('********************')
		PRINT (@sql_updateExisting)
		PRINT ('********************')
		EXEC sp_executesql @sql_updateExisting
	END
	IF (@par_Debug <> 1)
		EXEC sp_executesql @sql_updateExisting 
	BEGIN
		PRINT ('********************')
		PRINT (@sql_update)
		PRINT ('********************')
		EXEC [DA_MDDE].[sp_Logger] 'INFO', N'Run Query to load changed records.'
		EXEC sp_executesql @sql_update
	END
	IF (@par_Debug <> 1)
		EXEC sp_executesql @sql_update 
END TRY

BEGIN CATCH
    SELECT @ErrorMessage = ERROR_MESSAGE(),
           @ErrorSeverity = ERROR_SEVERITY(),
           @ErrorState = ERROR_STATE();

	SELECT ERROR_NUMBER() AS ErrorNumber, ERROR_MESSAGE() AS ErrorMessage;
	SET @LogMessage = CONCAT ('Error Message: ', @ErrorMessage)
	EXEC [DA_MDDE].[sp_Logger] 'ERROR', @LogMessage
	EXEC  [DA_MDDE].[sp_UpdateConfigExecution] @ExecutionId, 'LoadOutcome', 'Error'
    RAISERROR(@ErrorMessage, @ErrorSeverity, @ErrorState);
END CATCH
GO


