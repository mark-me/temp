CREATE PROC [DA_MDDE].[sp_LoadEntityData_FullLoad] 
	 @par_runid [NVARCHAR] (500)
	,@par_LayerName [NVARCHAR] (500)
	,@par_SourceName [NVARCHAR] (500)
	,@par_DestinationName [NVARCHAR] (500)
	,@par_MappingName [NVARCHAR] (500)
	,@par_Debug [Bit]
AS
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

***************************************************************************************************/
BEGIN TRY
	DECLARE @sql NVARCHAR(MAX) = ''
	DECLARE @LogMessage NVARCHAR(MAX);
	IF (@par_Debug = 1)
		BEGIN 
			EXEC [DA_MDDE].[sp_Logger] 'INFO', 'Debug is set to True'
		END

	BEGIN -- Truncate target table
		SET @LogMessage = CONCAT ('Going to truncate the table: ', '[', @par_LayerName, '].[', @par_DestinationName, ']')

		EXEC [DA_MDDE].[sp_Logger] 'INFO', @LogMessage

		SET @sql = CONCAT ('TRUNCATE TABLE ', '[', @par_LayerName, '].[', @par_DestinationName, ']')

		
	
		IF (@par_Debug = 1)
			BEGIN 
				EXEC [DA_MDDE].[sp_Logger] 'INFO', @sql
			END
		ELSE
			BEGIN
				EXEC sp_executesql @sql
			END
	END

	BEGIN -- Loading new records for source view
		DECLARE @sqlNewRow NVARCHAR(50) = CHAR(13) + CHAR(10)
		DECLARE @sqlRowcount NVARCHAR(MAX) = ''
		DECLARE @rowcount_New BIGINT
		DECLARE @rowcount_Update BIGINT

		SELECT @sql =  sqlcode 
		FROM (
			SELECT CONCAT(
					'INSERT INTO [',@par_LayerName,'].[',@par_DestinationName,']', CHAR(13),  CHAR(10)
				, '(', CHAR(13),  CHAR(10)
				, STRING_AGG(CHAR(9)+ dest.[name]   ,', '+ CHAR(13) + CHAR(10)) WITHIN GROUP (ORDER BY dest.column_id ASC), CHAR(13),  CHAR(10)
				, ')', CHAR(13),  CHAR(10)
				, 'SELECT ', CHAR(13),  CHAR(10)
				, STRING_AGG(CONCAT(CHAR(9), dest.[name], ' = ', 'source.', source.[name]) , ', ' + CHAR(13) + CHAR(10)) WITHIN GROUP (ORDER BY dest.column_id ASC), CHAR(13),  CHAR(10)
				, CONCAT('FROM [',@par_LayerName,'].[',@par_SourceName,'] as source', CHAR(13),  CHAR(10))
				, 'WHERE 1=1', CHAR(13),  CHAR(10)
				, CASE WHEN LOWER(left(@par_DestinationName, 4)) = 'aggr' 
					THEN '' 
					ELSE  CONCAT('AND NOT EXISTS (SELECT 1 FROM [', @par_LayerName , '].[', @par_DestinationName ,'] AS destination WHERE destination.', @par_DestinationName, 'BKey =  source.', @par_DestinationName, 'BKey)')
				 END
				) AS sqlcode
			FROM sys.columns dest
			LEFT JOIN sys.columns source ON source.object_id = OBJECT_ID(CONCAT(QUOTENAME(@par_LayerName),'.',QUOTENAME(@par_SourceName))) and dest.[name]  =  source.[name] 
			WHERE 
			dest.object_id = OBJECT_ID(CONCAT(QUOTENAME(@par_LayerName),'.',QUOTENAME(@par_DestinationName)))
			AND dest.is_identity = 0
		) a;
		
		
		SELECT @sqlRowcount =  sqlcode 
		FROM (
			SELECT CONCAT(
				  CONCAT('select @outputFromExec = count(*) FROM [',@par_LayerName,'].[',@par_SourceName,'] as source', CHAR(13),  CHAR(10))
				, 'WHERE 1=1', CHAR(13),  CHAR(10)
				, CASE WHEN LOWER(left(@par_DestinationName, 4)) = 'aggr' 
					THEN '' 
					ELSE  CONCAT('AND NOT EXISTS (SELECT 1 FROM [', @par_LayerName , '].[', @par_DestinationName ,'] AS destination WHERE destination.', @par_DestinationName, 'BKey =  source.', @par_DestinationName, 'BKey)')
				 END
				) AS sqlcode
			FROM sys.columns dest
			LEFT JOIN sys.columns source ON source.object_id = OBJECT_ID(CONCAT(QUOTENAME(@par_LayerName),'.',QUOTENAME(@par_SourceName))) and dest.[name]  =  source.[name] 
			WHERE 
			dest.object_id = OBJECT_ID(CONCAT(QUOTENAME(@par_LayerName),'.',QUOTENAME(@par_DestinationName)))
			AND dest.is_identity = 0
		) a;
		
		IF (@par_Debug = 1)
			BEGIN 
				EXEC [DA_MDDE].[sp_Logger] 'INFO', @sqlRowcount
			END
		ELSE
			BEGIN
				EXEC sp_executesql @sqlRowcount, N'@outputFromExec bigint out', @rowcount_New OUT
			END

		SET @LogMessage = CONCAT ('Rowcount to be inserted into ', '[', @par_LayerName, '].[', @par_DestinationName, ']', ' is: ', @rowcount_New)

		EXEC [DA_MDDE].[sp_Logger] 'INFO', @LogMessage

		SET @LogMessage = CONCAT ('Execute load command: ', @sqlNewRow, @sql, @sqlNewRow)

		EXEC [DA_MDDE].[sp_Logger] 'INFO', @LogMessage

		IF (@par_Debug = 1)
			BEGIN 
				EXEC [DA_MDDE].[sp_Logger] 'INFO', @sql
			END
		ELSE
			BEGIN
				EXEC sp_executesql @sql
			END
	END
END TRY

BEGIN CATCH
	SELECT ERROR_NUMBER() AS ErrorNumber, ERROR_MESSAGE() AS ErrorMessage;
END CATCH
GO