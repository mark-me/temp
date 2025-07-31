CREATE PROC [DA_MDDE].[sp_LoadEntity_StartExecution] @par_runid [NVARCHAR] (500)
AS
/***************************************************************************************************
Script Name         sp_LoadEntity_StartExecution.sql
Create Date:        2025-02-17
Author:             Jeroen Poll
Description:        PROC
Used By:            -
Parameter(s):       - 	  @par_runid [NVARCHAR] (500)	    = ETL RunID form the Synapse Pipeline. 
					
Usage:              -
****************************************************************************************************
SUMMARY OF CHANGES
Date(yyyy-mm-dd)    Author              Comments
------------------- ------------------- ------------------------------------------------------------
2025-07-28	        Jeroen Poll         Initial Script 

***************************************************************************************************/
SET NOCOUNT ON;

DECLARE @sql NVARCHAR(MAX) = ''

/* Declare params for Error Handling */
DECLARE @ErrorMessage NVARCHAR(4000), @ErrorSeverity INT, @ErrorState INT;

/* Declare params for logging */
DECLARE @LogID UNIQUEIDENTIFIER, @ObjectID BIGINT, @PipelineRunID NVARCHAR(36), @ActivityID NVARCHAR(36), @TriggerID NVARCHAR(36), @SourceCode NVARCHAR(10), @Object NVARCHAR(200), @State NVARCHAR(50), @User NVARCHAR(128), @PipelineName NVARCHAR(200), @TriggerName NVARCHAR(200), @TriggerType NVARCHAR(50), @StoredProcName NVARCHAR(200), @StoredProcParameter NVARCHAR(4000), @LogMessage NVARCHAR(4000)

/* Set Log params to default*/
SET @LogID = NEWID()
SET @ObjectID = NULL
SET @PipelineRunID = @par_runid
SET @ActivityID = NULL
SET @TriggerID = NULL
SET @SourceCode = NULL
SET @Object = NULL
SET @State = 'Started'
SET @User = CURRENT_USER
SET @PipelineName = NULL
SET @TriggerName = NULL
SET @TriggerType = NULL
SET @StoredProcName = 'DA_MDDE.sp_LoadEntity_StartExecution'
SET @StoredProcParameter = CONCAT (N'', '@par_runid: ', @par_runid)


BEGIN TRY
	/* Check if runid is unique */
	IF (SELECT COUNT(*) FROM [DA_MDDE].[ConfigExecution] WHERE [LoadRunId] = @par_runid) > 0 
		BEGIN	
			THROW 50000, N'ERROR: LoadRunId bestaat al in ConfigExecution tabel!', 1
		END
	/* Insert Config to Config Execution table */
	BEGIN
		INSERT INTO [DA_MDDE].[ConfigExecution] (
			[DateTime]
			, [LoadRunId]
			, [Model]
			, [Schema]
			, [Mapping]
			, [Source]
			, [Destination]
			, [RunLevel]
			, [RunLevelStage]
			, [LoadType]
			, [LoadStartDateTime]
			, [LoadEndDateTime]
			, [RowCountInsert]
			, [RowCountUpdate]
			, [RowCountDelete]
			, [LoadOutcome]
			)
		SELECT [DateTime] = GETDATE() AT TIME ZONE 'UTC' AT TIME ZONE 'W. Europe Standard Time'
			, [LoadRunId] = @par_runid
			, [Model] = [Model]
			, [Schema] = [Schema]
			, [Mapping] = [Mapping]
			, [Source] = [Source]
			, [Destination] = [Destination]
			, [RunLevel] = [RunLevel]
			, [RunLevelStage] = [RunLevelStage]
			, [LoadType] = [LoadType]
			, [LoadStartDateTime] = NULL
			, [LoadEndDateTime] = NULL
			, [RowCountInsert] = NULL
			, [RowCountUpdate] = NULL
			, [RowCountDelete] = NULL
			, [LoadOutcome] = NULL
		FROM [DA_MDDE].[ConfigBase]
	END

	/* Start Loading */
	BEGIN
		SET @LogMessage = CONCAT (N'','Begin laden. Kopieer Config naar ConfigExecution tabel' )

		EXEC [DA_MDDE].[sp_InsertLogRecord] @LogID,@ObjectID,@PipelineRunID,@ActivityID,@TriggerID,@SourceCode,@Object,@State,@User,@PipelineName,@TriggerName,@TriggerType,@StoredProcName,@StoredProcParameter,@LogMessage
	END

	BEGIN
		/*
			Check if predecessor(s) loaded successfully 
			If predecessor(s) are not "OK", then set loadoutcome to "Did Not Run" and stop.
			Else load the data.
		*/
		select 1 as aa
	END

		/* Einde Loading */
	BEGIN
		SET @LogMessage = CONCAT (N'','Einde laden. Kopieer Config naar ConfigExecution tabel')

		EXEC [DA_MDDE].[sp_InsertLogRecord] @LogID,@ObjectID,@PipelineRunID,@ActivityID,@TriggerID,@SourceCode,@Object,'Succeeded',@User,@PipelineName,@TriggerName,@TriggerType,@StoredProcName,@StoredProcParameter,@LogMessage
	END



END TRY

BEGIN CATCH
	/* Einde Loading State ERROR */
	BEGIN
		SET @LogMessage = CONCAT (N'','Error laden.')

		EXEC [DA_MDDE].[sp_InsertLogRecord] @LogID,@ObjectID,@PipelineRunID,@ActivityID,@TriggerID,@SourceCode,@Object,'Failed',@User,@PipelineName,@TriggerName,@TriggerType,@StoredProcName,@StoredProcParameter,@LogMessage
	END

	/* Einde Loading State ERROR */
	BEGIN
		SET @LogMessage = CONCAT (N'','ErroMessage: ', ERROR_MESSAGE())
		EXEC [DA_MDDE].[sp_InsertLogRecord] @LogID,@ObjectID,@PipelineRunID,@ActivityID,@TriggerID,@SourceCode,@Object,'Failed',@User,@PipelineName,@TriggerName,@TriggerType,@StoredProcName,@StoredProcParameter,@LogMessage
		SET @LogMessage = CONCAT (N'','ErrorSeverity: ', ERROR_SEVERITY())
		EXEC [DA_MDDE].[sp_InsertLogRecord] @LogID,@ObjectID,@PipelineRunID,@ActivityID,@TriggerID,@SourceCode,@Object,'Failed',@User,@PipelineName,@TriggerName,@TriggerType,@StoredProcName,@StoredProcParameter,@LogMessage
		SET @LogMessage = CONCAT (N'','ErrorState: ', ERROR_STATE())
		EXEC [DA_MDDE].[sp_InsertLogRecord] @LogID,@ObjectID,@PipelineRunID,@ActivityID,@TriggerID,@SourceCode,@Object,'Failed',@User,@PipelineName,@TriggerName,@TriggerType,@StoredProcName,@StoredProcParameter,@LogMessage

	END

	
	SELECT @ErrorMessage = ERROR_MESSAGE(), @ErrorSeverity = ERROR_SEVERITY(), @ErrorState = ERROR_STATE();
	RAISERROR (@ErrorMessage, @ErrorSeverity, @ErrorState);
END CATCH
GO