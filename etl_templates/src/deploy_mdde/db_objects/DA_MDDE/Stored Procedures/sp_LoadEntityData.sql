﻿CREATE PROC [DA_MDDE].[sp_LoadEntityData] @par_runid [NVARCHAR](500),@par_LayerName [NVARCHAR](500),@par_SourceName [NVARCHAR](500),@par_DestinationName [NVARCHAR](500),@par_MappingName [NVARCHAR](500),@par_loadtype [int],@par_DisableCheckColumnsAndDatatypes [bit] AS
/***************************************************************************************************
Script Name         sp_LoadEntityData.sql
Create Date:        2025-02-17
Author:             Jeroen Poll
Description:        PROC
Used By:            -
Parameter(s):       - 	  @par_runid [NVARCHAR] (500)			= ETL RunID form the Synapse Pipeline. 
						, @par_LayerName [NVARCHAR] (500)		= Schema Name. 
						, @par_SourceName [NVARCHAR] (500)		= Source table or view Name. 
						, @par_DestinationName [NVARCHAR] (500) = Desitination table or view Name. 
						, @par_MappingName [NVARCHAR] (500)		= Mapping name from PowerDesigner. Is only used for logging.  
						, @par_loadtype [Tinyint]  	0 = Entity Full Load
												1 = Entity Incremental Load
												2 = Dimension Table Full Load
												3 = Dimension Table Incremental Load
												4 = Fact Table Full Load
												5 = Fact Table Incremental Load 
												90 = Load Sample Set
												98 = Error Predecessor 
												99 = Disabled
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
***************************************************************************************************/
DECLARE @sql NVARCHAR(MAX) = ''
DECLARE @LogMessage NVARCHAR(max);

BEGIN TRY
	DECLARE @err_message nvarchar(max)
	IF (SELECT COUNT(*) FROM SYS.OBJECTS o WHERE 1 = 1 AND o.name = @par_SourceName AND schema_name(o.schema_id) = @par_LayerName)  = 0
		BEGIN
			SET @err_message = CONCAT(N'Source view ' , @par_LayerName, '.', @par_SourceName, ' does not exist.')
			RAISERROR(@err_message, 16, 1)
		END


	BEGIN
		SET @LogMessage = CONCAT (@par_runid,'¡','Begin laden van mapping: ', @par_MappingName)

		EXEC [DA_MDDE].[sp_Logger] 'INFO', @LogMessage
	END


	IF @par_DisableCheckColumnsAndDatatypes = 0
	BEGIN
		/* Check if source and destination tabel have same datatypes and length */
		BEGIN
			SET @LogMessage = CONCAT ('Begin check source and destination datatypes and length: ', CONCAT (QUOTENAME(@par_LayerName), '.', QUOTENAME(@par_SourceName)))

			EXEC [DA_MDDE].[sp_Logger] 'INFO', @LogMessage
		END

		DECLARE @checkColumnsAndDatatypes BIT

		SELECT @checkColumnsAndDatatypes = MIN(CASE 
					WHEN source.[name] IS NOT NULL AND source.system_type_id = dest.system_type_id AND source.max_length = dest.max_length
						THEN 1
					ELSE 0
					END)
		FROM sys.columns dest
		LEFT JOIN sys.columns source ON source.object_id = OBJECT_ID(CONCAT (QUOTENAME(@par_LayerName), '.', QUOTENAME(@par_SourceName))) AND dest.[name] = source.[name]
		WHERE dest.object_id = OBJECT_ID(CONCAT (QUOTENAME(@par_LayerName), '.', QUOTENAME(@par_DestinationName))) AND dest.is_identity = 0

		IF @checkColumnsAndDatatypes = 0
		BEGIN
			EXEC [DA_MDDE].[sp_PrintLoadingDatatypeCheck] @par_runid, @par_LayerName, @par_SourceName, @par_DestinationName
		END
	END

	BEGIN
		SET @LogMessage = CONCAT ('Begin loading Mapping: ', ISNULL(@par_LayerName, ''), '.', ISNULL(@par_DestinationName, ''), '.', ISNULL(@par_MappingName, ''))

		EXEC [DA_MDDE].[sp_Logger] 'INFO', @LogMessage
	END

	BEGIN
		SET @sql = CASE @par_loadtype
				WHEN 0 /* Full Load*/
					THEN CONCAT ('EXEC [DA_MDDE].[sp_LoadEntityData_FullLoad] ', QUOTENAME(@par_runid, ''''), ',', QUOTENAME(@par_LayerName, ''''), ',', QUOTENAME(@par_SourceName, ''''), ',', QUOTENAME(@par_DestinationName, ''''), ',', QUOTENAME(@par_MappingName, ''''), ', 0 , 0')
				WHEN 1 /* Incremental Load */
					THEN CONCAT ('EXEC [DA_MDDE].[sp_LoadEntityData_IncrementalLoad] ', QUOTENAME(@par_runid, ''''), ',', QUOTENAME(@par_LayerName, ''''), ',', QUOTENAME(@par_SourceName, ''''), ',', QUOTENAME(@par_DestinationName, ''''), ',', QUOTENAME(@par_MappingName, ''''), ', 0')
				WHEN 2 /* Dimension Table Full Load */
					THEN CONCAT ('EXEC [DA_MDDE].[sp_LoadEntityData_FullLoad] ', QUOTENAME(@par_runid, ''''), ',', QUOTENAME(@par_LayerName, ''''), ',', QUOTENAME(@par_SourceName, ''''), ',', QUOTENAME(@par_DestinationName, ''''), ',', QUOTENAME(@par_MappingName, ''''), ', 0')
				WHEN 3 /* Dimension Table Incremental Load */
					THEN CONCAT ('EXEC [DA_MDDE].[sp_LoadEntityData_IncrementalLoad] ', QUOTENAME(@par_runid, ''''), ',', QUOTENAME(@par_LayerName, ''''), ',', QUOTENAME(@par_SourceName, ''''), ',', QUOTENAME(@par_DestinationName, ''''), ',', QUOTENAME(@par_MappingName, ''''), ', 0')
				WHEN 4 /* Fact Table Full Load */
					THEN 'SELECT 1'
				WHEN 5 /* Fact Table Incremental Load */
					THEN 'SELECT 1'
				WHEN 90 /* Sample set with Full Load*/
					THEN CONCAT ('EXEC [DA_MDDE].[sp_LoadEntityData_FullLoad] ', QUOTENAME(@par_runid, ''''), ',', QUOTENAME(@par_LayerName, ''''), ',', QUOTENAME(@par_SourceName, ''''), ',', QUOTENAME(@par_DestinationName, ''''), ',', QUOTENAME(@par_MappingName, ''''), ', 1 , 0')
				WHEN 98 /* Error Predecessor */
					THEN 'SELECT 1'
				WHEN 99 /*  Disabled */
					THEN 'SELECT 1'
				ELSE 'SELECT 1'
				END
		SET @LogMessage = CASE @par_loadtype
				WHEN -1
					THEN CONCAT (@par_runid,'¡','LoadType is set to: Sample Set (Full Load), with command: ', @sql)
				WHEN 0
					THEN CONCAT (@par_runid,'¡','LoadType is set to: Full Load, with command: ', @sql)
				WHEN 1
					THEN CONCAT (@par_runid,'¡','LoadType is set to: Incremental Load, with command: ', @sql)
				WHEN 2
					THEN CONCAT (@par_runid,'¡','LoadType is set to: Dimension Table Full Load, with command: ', @sql)
				WHEN 3
					THEN CONCAT (@par_runid,'¡','LoadType is set to: Dimension Table Incremental Load, with command: ', @sql)
				WHEN 4
					THEN CONCAT (@par_runid,'¡','LoadType is set to: Fact Table Full Load, with command: ', @sql)
				WHEN 5
					THEN CONCAT (@par_runid,'¡','LoadType is set to: Fact Table Incremental Load, with command: ', @sql)
				WHEN 98
					THEN CONCAT (@par_runid,'¡','LoadType is set to: Error Predecessor, with command: ', @sql)
				WHEN 99
					THEN CONCAT (@par_runid,'¡','LoadType is set to: Disabled', '.')
				ELSE 'SELECT 1'
				END

		EXEC [DA_MDDE].[sp_Logger] 'INFO', @LogMessage;
		EXEC sp_executesql @sql;
	END

	/* Initialize Dimension. Add dummy record to Dim table */
	IF (@par_loadtype = 2 OR @par_loadtype = 3)
		BEGIN
			EXEC [DA_MDDE].[sp_Logger] 'INFO', 'Executing PROC: sp_InitializeDimension.'
			EXEC [DA_MDDE].[sp_InitializeDimension] @par_runid, @par_LayerName, @par_DestinationName, 0
		END
END TRY

BEGIN CATCH
	DECLARE @ErrorMessage NVARCHAR(4000),
            @ErrorSeverity INT,
            @ErrorState INT;

    SELECT @ErrorMessage = ERROR_MESSAGE(),
           @ErrorSeverity = ERROR_SEVERITY(),
           @ErrorState = ERROR_STATE();
	/* Update config table with NOK, timestamp and run sp_UpdateConfig_ErrorPredecessor to update predecessors */
	SET @LogMessage = CONCAT (@par_runid,'¡',N'Update Config Table with uitcome NOK')
	EXEC [DA_MDDE].[sp_Logger] 'INFO', @LogMessage
	EXEC [DA_MDDE].[sp_UpdateConfig_Error]  @par_runid , @par_LayerName , @par_MappingName , 0 

	SET @LogMessage = CONCAT ('Error Message: ', @ErrorMessage)
	EXEC [DA_MDDE].[sp_Logger] 'ERROR', @LogMessage
	SELECT ERROR_NUMBER() AS ErrorNumber, ERROR_MESSAGE() AS ErrorMessage;
    RAISERROR(@ErrorMessage, @ErrorSeverity, @ErrorState);
END CATCH
GO