CREATE PROC [DA_MDDE].[sp_LoadEntity_Full] @par_runid [NVARCHAR](500),@par_Schema [NVARCHAR](500),@par_Source [NVARCHAR](500),@par_Destination [NVARCHAR](500),@par_Mapping [NVARCHAR](500),@par_SampleSet [Bit] ,@par_Debug [Bit] AS

/***************************************************************************************************
Script Name         sp_LoadEntity_Full.sql
Create Date:        2025-02-17
Author:             Jeroen Poll
Description:        PROC
Used By:            -
Parameter(s):       - @par_runid [NVARCHAR] (500)  /* ETL RunID form the Synapse Pipeline. */
					, @par_Schema [NVARCHAR] (500)  /* Schema Name. */
					, @par_Source [NVARCHAR] (500) /* Source table or view Name. */
					, @par_Destination [NVARCHAR] (500) /* Desitination table or view Name. */
					, @par_Mapping [NVARCHAR] (500) /* Mapping name from PowerDesigner. Is only used for logging. */
					, @par_Debug [Boolean] /* If true, only statement will be printed and not executed. */
					, @par_SampleSet  [Boolean] /* If true, top 100 will be loaded for tables */
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
2025-07-17			Jeroen Poll			Add support for Loading sample sets
2025-08-01			Jeroen Poll			Alter script for new loading methode and new Logger
***************************************************************************************************/
BEGIN TRY
	SET NOCOUNT ON;
	DECLARE @sql NVARCHAR(MAX) = ''
	DECLARE @sql_select NVARCHAR(MAX) = ''
	DECLARE @sql_insert NVARCHAR(MAX) = ''
	DECLARE @sql_filter NVARCHAR(MAX) = ''
	DECLARE @LoadType_UniqueKey INT
	DECLARE @LoadType_BKey BIT
	DECLARE @ExecutionId UNIQUEIDENTIFIER = newid()
	DECLARE @sqlNewRow NVARCHAR(50) = CHAR(13) + CHAR(10)
	DECLARE @sqlRowcount NVARCHAR(MAX) = ''
	DECLARE @rowcount_New BIGINT
	DECLARE @rowcount_Update BIGINT
	DECLARE @sel NVARCHAR(max)
	DECLARE @ErrorMessage NVARCHAR(4000);
	DECLARE @ErrorSeverity INT;
	DECLARE @ErrorState INT;

	/* Declare params for logging */
	DECLARE @LogID UNIQUEIDENTIFIER
	DECLARE @ObjectID BIGINT
	DECLARE @PipelineRunID NVARCHAR(36)
	DECLARE @ActivityID NVARCHAR(36)
	DECLARE @TriggerID NVARCHAR(36)
	DECLARE @SourceCode NVARCHAR(200)
	DECLARE @Object NVARCHAR(200)
	DECLARE @State NVARCHAR(50)
	DECLARE @User NVARCHAR(128)
	DECLARE @PipelineName NVARCHAR(200)
	DECLARE @TriggerName NVARCHAR(200)
	DECLARE @TriggerType NVARCHAR(50)
	DECLARE @StoredProcName NVARCHAR(200)
	DECLARE @StoredProcParameter NVARCHAR(4000)
	DECLARE @LogMessage NVARCHAR(4000)
	/* Set Log params to default*/
	SET @LogID = NEWID()
	SET @ObjectID = NULL
	SET @PipelineRunID = @par_runid
	SET @ActivityID = NULL
	SET @TriggerID = NULL
	SET @SourceCode = @par_Source
	SET @Object = @par_Destination
	SET @State = 'Started'
	SET @User = CURRENT_USER
	SET @PipelineName = NULL
	SET @TriggerName = NULL
	SET @TriggerType = NULL
	SET @StoredProcName = 'DA_MDDE.sp_LoadEntity_Full'
	SET @StoredProcParameter = CONCAT (N'@par_runid: ', @par_runid, ', @par_Schema: ', @par_Schema, ', @par_Mapping: ', @par_Mapping, ', @par_Source: ', @par_Source ,', @par_Destination: ', @par_Destination, ', @par_SampleSet: ', @par_SampleSet, ',@par_Debug: ', @par_Debug)

	IF (@par_Debug = 1)
		BEGIN 
			SET @LogMessage = CONCAT (N'', 'Debug is set to True')
			EXEC [DA_MDDE].[sp_InsertLogRecord] @LogID,@ObjectID,@PipelineRunID,@ActivityID,@TriggerID,@SourceCode,@Object,@State,@User,@PipelineName,@TriggerName,@TriggerType,@StoredProcName,@StoredProcParameter,@LogMessage
		END

	/* Check if table has Unique Key constraint, needed for loading the data */
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
				CASE WHEN @par_SampleSet = 1 AND (LEFT(@par_Mapping,7) = 'Da_MDDE' OR LEFT(@par_Destination,4) = 'Aggr' ) THEN 'SELECT  '
					 WHEN @par_SampleSet = 1 AND NOT (LEFT(@par_Mapping,7) = 'Da_MDDE' OR LEFT(@par_Destination,4) = 'Aggr' )   THEN 'SELECT TOP 100 ' 
					 ELSE 'SELECT ' END
				, CHAR(13), CHAR(10), STRING_AGG(CONCAT (
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
		EXEC [DA_MDDE].[sp_InsertLogRecord] @LogID,@ObjectID,@PipelineRunID,@ActivityID,@TriggerID,@SourceCode,@Object,@State,@User,@PipelineName,@TriggerName,@TriggerType,@StoredProcName,@StoredProcParameter,@ErrorMessage
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
					'AND NOT EXISTS (SELECT 1 FROM [', @par_Schema, '].[', @par_Destination, '] AS destination WHERE ', STRING_AGG(CONCAT ('destination.[', cc.COLUMN_NAME, '] = source.[', cc.COLUMN_NAME, ']'), ' AND ') WITHIN GROUP (
							ORDER BY cc.COLUMN_NAME ASC
							), ')'
					) AS sqlcode
			FROM information_schema.TABLE_CONSTRAINTS c
			INNER JOIN information_schema.CONSTRAINT_COLUMN_USAGE cc ON c.TABLE_SCHEMA = cc.TABLE_SCHEMA AND c.TABLE_NAME = cc.TABLE_NAME AND c.CONSTRAINT_NAME = cc.CONSTRAINT_NAME
			WHERE 1 = 1 AND c.TABLE_SCHEMA = @par_Schema AND c.TABLE_NAME = @par_Destination AND c.CONSTRAINT_TYPE = 'UNIQUE'
			) a;
	END
	ELSE IF @LoadType_BKey = 1
	BEGIN
		SELECT @sql_filter = sqlcode
		FROM (
			SELECT CONCAT (
					N'', CASE 
						WHEN LOWER(left(@par_Destination, 4)) = 'aggr'
							THEN N''
						ELSE CONCAT ('AND NOT EXISTS (SELECT 1 FROM [', @par_Schema, '].[', @par_Destination, '] AS destination WHERE destination.[', @par_Destination, 'BKey] =  source.[', @par_Destination, 'BKey])')
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
			SET @LogMessage = @sqlRowcount
			EXEC [DA_MDDE].[sp_InsertLogRecord] @LogID,@ObjectID,@PipelineRunID,@ActivityID,@TriggerID,@SourceCode,@Object,@State,@User,@PipelineName,@TriggerName,@TriggerType,@StoredProcName,@StoredProcParameter,@LogMessage
		END
	ELSE
		BEGIN
			SET @LogMessage = N'Run Query to get rowcount for new insert.'
			EXEC [DA_MDDE].[sp_InsertLogRecord] @LogID,@ObjectID,@PipelineRunID,@ActivityID,@TriggerID,@SourceCode,@Object,@State,@User,@PipelineName,@TriggerName,@TriggerType,@StoredProcName,@StoredProcParameter,@LogMessage

			EXEC sp_executesql @sqlRowcount, N'@outputFromExec bigint out', @rowcount_New OUT
		END

	
	/* Update rowcount in Config table */
	SET @LogMessage = N'Update Config table with Rowcounts.'
	EXEC [DA_MDDE].[sp_InsertLogRecord] @LogID,@ObjectID,@PipelineRunID,@ActivityID,@TriggerID,@SourceCode,@Object,@State,@User,@PipelineName,@TriggerName,@TriggerType,@StoredProcName,@StoredProcParameter,@LogMessage

	SET @LogMessage = CONCAT (N'Rowcount to be inserted into ', '[', @par_Schema, '].[', @par_Destination, ']', ' is: ', @rowcount_New)
	EXEC [DA_MDDE].[sp_InsertLogRecord] @LogID,@ObjectID,@PipelineRunID,@ActivityID,@TriggerID,@SourceCode,@Object,@State,@User,@PipelineName,@TriggerName,@TriggerType,@StoredProcName,@StoredProcParameter,@LogMessage

	IF (@par_Debug = 1)
	BEGIN
		SET @LogMessage = @sql
		EXEC [DA_MDDE].[sp_InsertLogRecord] @LogID,@ObjectID,@PipelineRunID,@ActivityID,@TriggerID,@SourceCode,@Object,@State,@User,@PipelineName,@TriggerName,@TriggerType,@StoredProcName,@StoredProcParameter,@LogMessage
	END
	ELSE
	BEGIN
		SET @LogMessage = N'Run Query for new insert.'
		EXEC [DA_MDDE].[sp_InsertLogRecord] @LogID,@ObjectID,@PipelineRunID,@ActivityID,@TriggerID,@SourceCode,@Object,@State,@User,@PipelineName,@TriggerName,@TriggerType,@StoredProcName,@StoredProcParameter,@LogMessage

		EXEC sp_executesql @sql
	END
		
	/* Update config table with OK and timestamp */
	SET @LogMessage = N'Update Config table with sp_UpdateConfig_End.'
	EXEC [DA_MDDE].[sp_InsertLogRecord] @LogID,@ObjectID,@PipelineRunID,@ActivityID,@TriggerID,@SourceCode,@Object,@State,@User,@PipelineName,@TriggerName,@TriggerType,@StoredProcName,@StoredProcParameter,@LogMessage
		
	EXEC [DA_MDDE].[sp_EndEntity_Execution] @par_runid,@par_schema ,@par_mapping , @rowcount_New , 0,0
END TRY

BEGIN CATCH
    SELECT @ErrorMessage = ERROR_MESSAGE(),
           @ErrorSeverity = ERROR_SEVERITY(),
           @ErrorState = ERROR_STATE();

	SELECT ERROR_NUMBER() AS ErrorNumber, ERROR_MESSAGE() AS ErrorMessage;
	/* Update config table with NOK, timestamp and run sp_UpdateConfig_ErrorPredecessor to update predecessors */
	SET @LogMessage = CONCAT ('',N'Update Config Table with uitcome NOK')
	EXEC [DA_MDDE].[sp_InsertLogRecord] @LogID,@ObjectID,@PipelineRunID,@ActivityID,@TriggerID,@SourceCode,@Object,@State,@User,@PipelineName,@TriggerName,@TriggerType,@StoredProcName,@StoredProcParameter,@LogMessage
	EXEC [DA_MDDE].[sp_UpdateEntity_Execution] @par_runid ,@par_schema ,@par_mapping  ,'NOK'

    RAISERROR(@ErrorMessage, @ErrorSeverity, @ErrorState);
END CATCH
GO