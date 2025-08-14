DROP  PROC [DA_MDDE].[sp_LoadEntity]
GO
CREATE PROC [DA_MDDE].[sp_LoadEntity] @par_runid [NVARCHAR] (500), @par_Schema [NVARCHAR] (500), @par_Mapping [NVARCHAR] (500), @par_Source [NVARCHAR] (500), @par_Destination [NVARCHAR] (500), @par_loadtype [int], @par_DisableCheckColumnsAndDatatypes [bit]
AS
/***************************************************************************************************
Script Name         sp_LoadEntity.sql
Create Date:        2025-02-17
Author:             Jeroen Poll
Description:        PROC
Used By:            -
Parameter(s):       - 	  @par_runid [NVARCHAR] (500)	    = ETL RunID form the Synapse Pipeline. 
						, @par_Schema [NVARCHAR] (500)		= Schema Name. 
						, @par_Mapping [NVARCHAR] (500)		= Mapping name from PowerDesigner
						, @par_Source [NVARCHAR] (500)		= Source table or view Name. 
						, @par_Destination [NVARCHAR] (500) = Desitination table or view Name. 
						, @par_loadtype [Tinyint]  	0 = Entity Full Load
												1 = Entity Incremental Load
						, @par_DisableCheckColumnsAndDatatypes  [bit]  1 = Disable check on datatypes 
																	   0 = Check source and destination on loading
												
Usage:              -
****************************************************************************************************
SUMMARY OF CHANGES
Date(yyyy-mm-dd)    Author              Comments
------------------- ------------------- ------------------------------------------------------------
2025-02-17	        Jeroen Poll         Initial Script V1.0   First version Full Load Only
2025-02-28	        Jeroen Poll         V2.0   Added Incremental load 
2025-04-04	        Jeroen Poll         V2.1   Added checks and more load types
2025-07-11			Jeroen Poll			v2.2   Added Load option 98 for Error on Predecessor
2025-07-17			Jeroen Poll			v2.2   Added Load option 90 for sample sets
2025-07-25			Jeroen Poll			v3.0   rebuild and rename to sp_LoadEntity
***************************************************************************************************/
SET NOCOUNT ON;
DECLARE @sql NVARCHAR(MAX) = ''
DECLARE @par_model [NVARCHAR] (500) = @par_Schema

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
SET @StoredProcName = 'DA_MDDE.sp_LoadEntity'
SET @StoredProcParameter = CONCAT (N'@par_runid: ', @par_runid, ', @par_Schema: ', @par_Schema, ', @par_Mapping: ', @par_Mapping, ', @par_Source: ', @par_Source ,', @par_Destination: ', @par_Destination, ', @par_loadtype: ', @par_loadtype, ',@par_DisableCheckColumnsAndDatatypes: ', @par_DisableCheckColumnsAndDatatypes)

BEGIN TRY
	/* Check if Sourceview exists */
	DECLARE @err_message NVARCHAR(max)

	IF (
			SELECT COUNT(*)
			FROM SYS.OBJECTS o
			WHERE 1 = 1 AND o.name = @par_Source AND schema_name(o.schema_id) = @par_Schema
			) = 0
	BEGIN
		SET @err_message = CONCAT (N'Source view ', @par_Schema, '.', @par_Source, ' does not exist.')

		RAISERROR (@err_message, 16, 1)
	END

	/* Start Loading mapping voor entity */
	BEGIN
		SET @LogMessage = CONCAT (N'Begin laden van mapping: ', '''', @par_Mapping, '''', ' voor tabel: ', QUOTENAME(@par_Schema), '.', QUOTENAME(@par_Destination))

		EXEC [DA_MDDE].[sp_InsertLogRecord] @LogID,@ObjectID,@PipelineRunID,@ActivityID,@TriggerID,@SourceCode,@Object,@State,@User,@PipelineName,@TriggerName,@TriggerType,@StoredProcName,@StoredProcParameter,@LogMessage

		EXEC [DA_MDDE].[sp_StartEntity_Execution] @par_runid ,@par_schema ,@par_mapping 
	END

	/* Check if source with destination (datatype and length). */
	IF @par_DisableCheckColumnsAndDatatypes = 0
	BEGIN
		/* Check if source and destination tabel have same datatypes and length */
		BEGIN
			SET @LogMessage = CONCAT ('Begin check source and destination datatypes and length: ', CONCAT (QUOTENAME(@par_Schema), '.', QUOTENAME(@par_Source)))

			EXEC [DA_MDDE].[sp_InsertLogRecord] @LogID,@ObjectID,@PipelineRunID,@ActivityID,@TriggerID,@SourceCode,@Object,@State,@User,@PipelineName,@TriggerName,@TriggerType,@StoredProcName,@StoredProcParameter,@LogMessage
		END

		DECLARE @checkColumnsAndDatatypes BIT

		SELECT @checkColumnsAndDatatypes = MIN(CASE 
					WHEN source.[name] IS NOT NULL AND source.system_type_id = dest.system_type_id AND source.max_length = dest.max_length
						THEN 1
					ELSE 0
					END)
		FROM sys.columns dest
		LEFT JOIN sys.columns source ON source.object_id = OBJECT_ID(CONCAT (QUOTENAME(@par_Schema), '.', QUOTENAME(@par_Source))) AND dest.[name] = source.[name]
		WHERE dest.object_id = OBJECT_ID(CONCAT (QUOTENAME(@par_Schema), '.', QUOTENAME(@par_Destination))) AND dest.is_identity = 0

		IF @checkColumnsAndDatatypes = 0
		BEGIN
			EXEC [DA_MDDE].[sp_PrintLoadingDatatypeCheck] @par_runid, @par_Schema, @par_Source, @par_Destination
		END
	END

/*
	Check if predecessor(s) loaded successfully 
	If predecessor(s) are not "OK", then set loadoutcome to "Did Not Run" and stop.
	Else load the data.
*/
	DECLARE @checkPrecedingNOK BIT
	SELECT @checkPrecedingNOK = 	COUNT(*)
								--b.[Model],
								--b.[Schema],
								--b.[Mapping],

								--c.[Model] AS PrecedingModel,
								--c.[Schema] AS PrecedingSchema,
								--c.[Mapping] AS PrecedingMapping,
								--ISNULL(c.[LoadOutcome],'N/A') AS PrecedingOutcome
								FROM [DA_MDDE].[ConfigExecution] b
								INNER JOIN [DA_MDDE].[LoadDependencies] d ON b.[Model] = d.[Model] AND b.[Mapping] = d.[Mapping]
								INNER JOIN [DA_MDDE].[ConfigExecution] c ON c.[Model] = d.[PrecedingModel] AND c.[Mapping] = d.[PrecedingMapping] AND c.[LoadRunId] = @par_runid
								WHERE b.[LoadRunId] = @par_runid AND b.[Model] = @par_model AND b.[Mapping] = @par_Mapping
								AND ISNULL(c.[LoadOutcome],'N/A') <> 'OK'

	SET @LogMessage = CONCAT('Check Load Predecending NOK: ' ,@checkPrecedingNOK)
	EXEC [DA_MDDE].[sp_InsertLogRecord] @LogID,@ObjectID,@PipelineRunID,@ActivityID,@TriggerID,@SourceCode,@Object,@State,@User,@PipelineName,@TriggerName,@TriggerType,@StoredProcName,@StoredProcParameter,@LogMessage
	
	
	
	IF @checkPrecedingNOK > 0
		BEGIN
			SELECT 				b.[Model],
								b.[Schema],
								b.[Mapping],

								c.[Model] AS PrecedingModel,
								c.[Schema] AS PrecedingSchema,
								c.[Mapping] AS PrecedingMapping,
								ISNULL(c.[LoadOutcome],'N/A') AS PrecedingOutcome
								FROM [DA_MDDE].[ConfigExecution] b
								INNER JOIN [DA_MDDE].[LoadDependencies] d ON b.[Model] = d.[Model] AND b.[Mapping] = d.[Mapping]
								INNER JOIN [DA_MDDE].[ConfigExecution] c ON c.[Model] = d.[PrecedingModel] AND c.[Mapping] = d.[PrecedingMapping] AND c.[LoadRunId] = @par_runid
								WHERE b.[LoadRunId] = @par_runid AND b.[Model] = @par_model AND b.[Mapping] = @par_Mapping
								AND ISNULL(c.[LoadOutcome],'N/A') <> 'OK'

			EXEC [DA_MDDE].[sp_UpdateEntity_Execution] @par_runid, @par_schema, @par_mapping, 'Did Not Start'

			SET @LogMessage = CONCAT (N'', 'One or more Preceding mappings has a loadoutcome <> OK ')
			EXEC [DA_MDDE].[sp_InsertLogRecord] @LogID,@ObjectID,@PipelineRunID,@ActivityID,@TriggerID,@SourceCode,@Object,@State,@User,@PipelineName,@TriggerName,@TriggerType,@StoredProcName,@StoredProcParameter,@LogMessage
			SET @LogMessage = CONCAT ('Update Config Table with outcome ''Did Not Start'': ', CONCAT (QUOTENAME(@par_Schema), '.', QUOTENAME(@par_Source)))
			EXEC [DA_MDDE].[sp_InsertLogRecord] @LogID,@ObjectID,@PipelineRunID,@ActivityID,@TriggerID,@SourceCode,@Object,@State,@User,@PipelineName,@TriggerName,@TriggerType,@StoredProcName,@StoredProcParameter,@LogMessage
		END
	IF @checkPrecedingNOK = 0
		BEGIN
			SET @sql = CASE @par_loadtype
						WHEN 0 /* Full Load*/
							THEN CONCAT ('EXEC [DA_MDDE].[sp_LoadEntity_Full] ', QUOTENAME(@par_runid, ''''), ',', QUOTENAME(@par_Schema, ''''), ',', QUOTENAME(@par_Source, ''''), ',', QUOTENAME(@par_Destination, ''''), ',', QUOTENAME(@par_Mapping, ''''), ', 0 , 0')
						WHEN 1 /* Incremental Load */
							THEN CONCAT ('EXEC [DA_MDDE].[sp_LoadEntity_Incremental] ', QUOTENAME(@par_runid, ''''), ',', QUOTENAME(@par_Schema, ''''), ',', QUOTENAME(@par_Source, ''''), ',', QUOTENAME(@par_Destination, ''''), ',', QUOTENAME(@par_Mapping, ''''), ', 0')
						END
			EXEC sp_executesql @sql;
		END
END TRY

BEGIN CATCH
	DECLARE @ErrorMessage NVARCHAR(4000), @ErrorSeverity INT, @ErrorState INT;

	SELECT @ErrorMessage = ERROR_MESSAGE(), @ErrorSeverity = ERROR_SEVERITY(), @ErrorState = ERROR_STATE();

	/* Update config table with NOK, timestamp and run sp_UpdateConfig_ErrorPredecessor to update predecessors */
	SET @LogMessage = CONCAT ('Update Config Table with outcome NOK: ', CONCAT (QUOTENAME(@par_Schema), '.', QUOTENAME(@par_Source)))
	EXEC [DA_MDDE].[sp_InsertLogRecord] @LogID,@ObjectID,@PipelineRunID,@ActivityID,@TriggerID,@SourceCode,@Object,@State,@User,@PipelineName,@TriggerName,@TriggerType,@StoredProcName,@StoredProcParameter,@LogMessage
	EXEC [DA_MDDE].[sp_ErrorEntity_Execution] @par_runid, @par_schema ,@par_mapping , 0, 0 ,0
	SET @LogMessage = CONCAT ('Error Message: ', @ErrorMessage)
	EXEC [DA_MDDE].[sp_InsertLogRecord] @LogID,@ObjectID,@PipelineRunID,@ActivityID,@TriggerID,@SourceCode,@Object,@State,@User,@PipelineName,@TriggerName,@TriggerType,@StoredProcName,@StoredProcParameter,@LogMessage

	SELECT ERROR_NUMBER() AS ErrorNumber, ERROR_MESSAGE() AS ErrorMessage;

	RAISERROR (@ErrorMessage, @ErrorSeverity, @ErrorState);
END CATCH
GO