CREATE PROC [DA_MDDE].[sp_LoadEntityData_IncrementalLoad] @par_runid [NVARCHAR](500),@par_LayerName [NVARCHAR](500),@par_SourceName [NVARCHAR](500),@par_DestinationName [NVARCHAR](500),@par_MappingName [NVARCHAR](500),@par_Debug [Bit] AS

/***************************************************************************************************
Script Name         sp_LoadEntityData_IncrementalLoad.sql
Create Date:        2025-02-17
Author:             Jeroen Poll
Description:        PROC
Used By:            -
Parameter(s):       - @par_runid [NVARCHAR] (500)  /* ETL RunID form the Synapse Pipeline. */
					, @par_LayerName [NVARCHAR] (500)  /* Schema Name. */
					, @par_SourceName [NVARCHAR] (500) /* Source table or view Name. */
					, @par_DestinationName [NVARCHAR] (500) /* Desitination table or view Name. */
					, @par_MappingName [NVARCHAR] (500) /* Mapping name from PowerDesigner. Is only used for logging.  */
Usage:              -
****************************************************************************************************
SUMMARY OF CHANGES
Date(yyyy-mm-dd)    Author              Comments
------------------- ------------------- ------------------------------------------------------------
2025-02-17	        Jeroen Poll         Initial Script V1.0   First version Delta Load
2025-04-04			Jeroen Poll			Add column names.
2025-04-22			Avinash Kalicharan	Change SP name to 'Incremental'
2025-04-22			Avinash Kalicharan 	Add debug to the procedure

***************************************************************************************************/
BEGIN TRY
	DECLARE @sql NVARCHAR(MAX) = ''
	DECLARE @LogMessage NVARCHAR(MAX);
		IF (@par_Debug = 1)
			BEGIN 
				EXEC [DA_MDDE].[sp_Logger] 'INFO', 'Debug is set to True'
			END
		ELSE
			BEGIN
				EXEC sp_executesql @sql
			END
	BEGIN -- Loading new records for source view
		DECLARE @sqlNewRow NVARCHAR(50) = CHAR(13) + CHAR(10)
		DECLARE @sqlRowcount NVARCHAR(MAX) = ''
		DECLARE @rowcount_New BIGINT
		DECLARE @rowcount_Update BIGINT
		DECLARE @rowcount_Update_Insert BIGINT

		/* Insert new rows. (BKey does not exist in target table) */
		BEGIN
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
					, CONCAT('AND NOT EXISTS (SELECT 1 FROM [', @par_LayerName , '].[', @par_DestinationName ,'] AS destination WHERE destination.', @par_DestinationName, 'BKey =  source.', @par_DestinationName, 'BKey)')
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
					, CONCAT('AND NOT EXISTS (SELECT 1 FROM [', @par_LayerName , '].[', @par_DestinationName ,'] AS destination WHERE destination.', @par_DestinationName, 'BKey =  source.', @par_DestinationName, 'BKey)')
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
				SET @rowcount_New = null
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

		-- Update rows where BKey exist and X_HashKey not the same.
		BEGIN
			SET @sql = ''
			SET @sql = @sql +  CONCAT('UPDATE ', '[', @par_LayerName, '].[', @par_DestinationName, ']', @sqlNewRow)
			SET @sql = @sql +  CONCAT('SET [X_EndDate] = getdate()-1', @sqlNewRow)
			SET @sql = @sql +  CONCAT('   ,[X_IsCurrent] = 0', @sqlNewRow)
			SET @sql = @sql +  CONCAT('   ,[X_IsReplaced] = 1', @sqlNewRow)
			SET @sql = @sql +  CONCAT('FROM ', '[', @par_LayerName, '].[', @par_SourceName , ']', ' AS source', @sqlNewRow)
			SET @sql = @sql +  CONCAT('INNER JOIN ', '[', @par_LayerName, '].[', @par_DestinationName, ']', ' AS target',  @sqlNewRow)
			SET @sql = @sql +  CONCAT('ON source.', @par_DestinationName, 'BKey' , ' = ', 'target.', @par_DestinationName, 'BKey', ' AND source.[X_HashKey] <> target.[X_HashKey] AND target.[X_IsCurrent] = 1', @sqlNewRow)


			SET @sqlRowcount = ''
			SET @sqlRowcount = CONCAT ('select @outputFromExec = count(*) FROM ', '[', @par_LayerName, '].[',  @par_SourceName, ']', ' AS source', @sqlNewRow)
			SET @sqlRowcount = @sqlRowcount +  CONCAT('INNER JOIN ', '[', @par_LayerName, '].[', @par_DestinationName, ']', ' AS target',  @sqlNewRow)
			SET @sqlRowcount = @sqlRowcount +  CONCAT('ON source.', @par_DestinationName, 'BKey' , ' = ', 'target.', @par_DestinationName, 'BKey', ' AND source.[X_HashKey] <> target.[X_HashKey] AND target.[X_IsCurrent] = 1', @sqlNewRow)
			--PRINT(@sqlRowcount)
			IF (@par_Debug = 1)
				BEGIN 
					EXEC [DA_MDDE].[sp_Logger] 'INFO', @sql
				END
			ELSE
				BEGIN
					SET @rowcount_Update = null
					EXEC sp_executesql @sqlRowcount, N'@outputFromExec bigint out', @rowcount_Update OUT
				END

			SET @LogMessage = CONCAT ('Rowcount to be updated in  ', '[', @par_LayerName, '].[', @par_DestinationName, ']', ' is: ', @rowcount_Update)

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

		-- Insert updated rows. (BKey exist in target table and X_HashKey not the same )
		BEGIN
			SELECT @sql =  sqlcode 
			FROM (
				SELECT CONCAT(
						'INSERT INTO [', @par_LayerName , '].[', @par_DestinationName ,']', CHAR(13),  CHAR(10)
					, '(', CHAR(13),  CHAR(10)
					, STRING_AGG(CHAR(9)+ dest.[name]   ,', '+ CHAR(13) + CHAR(10)) WITHIN GROUP (ORDER BY dest.column_id ASC), CHAR(13),  CHAR(10)
					, ')', CHAR(13),  CHAR(10)
					, 'SELECT ', CHAR(13),  CHAR(10)
					, STRING_AGG(CONCAT(CHAR(9), dest.[name], ' = ', 'source.', source.[name]) , ', ' + CHAR(13) + CHAR(10)) WITHIN GROUP (ORDER BY dest.column_id ASC), CHAR(13),  CHAR(10)
					, CONCAT('FROM [',@par_LayerName,'].[',@par_SourceName,'] as source', CHAR(13),  CHAR(10))
					, 'WHERE 1=1', CHAR(13),  CHAR(10)
					, CONCAT('AND EXISTS (SELECT 1 FROM [', @par_LayerName , '].[', @par_DestinationName ,'] AS destination WHERE destination.', @par_DestinationName, 'BKey =  source.', @par_DestinationName, 'BKey AND source.[X_HashKey] <> destination.[X_HashKey] AND destination.[X_IsCurrent] = 1)')
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
					, CONCAT('AND EXISTS (SELECT 1 FROM [', @par_LayerName , '].[', @par_DestinationName ,'] AS destination WHERE destination.', @par_DestinationName, 'BKey =  source.', @par_DestinationName, 'BKey AND source.[X_HashKey] <> destination.[X_HashKey] AND destination.[X_IsCurrent] = 1)')
					) AS sqlcode
				FROM sys.columns dest
				LEFT JOIN sys.columns source ON source.object_id = OBJECT_ID(CONCAT(QUOTENAME(@par_LayerName),'.',QUOTENAME(@par_SourceName))) and dest.[name]  =  source.[name] 
				WHERE 
				dest.object_id = OBJECT_ID(CONCAT(QUOTENAME(@par_LayerName),'.',QUOTENAME(@par_DestinationName)))
				AND dest.is_identity = 0
			) a;
			
			IF (@par_Debug = 1)
				BEGIN 
					EXEC [DA_MDDE].[sp_Logger] 'INFO', @sql
				END
			ELSE
				BEGIN
					SET @rowcount_Update_Insert = null
					EXEC sp_executesql @sqlRowcount, N'@outputFromExec bigint out', @rowcount_Update_Insert OUT
				END
			SET @LogMessage = CONCAT ('Rowcount to be inserted (for updated rows) into ', '[', @par_LayerName, '].[', @par_DestinationName, ']', ' is: ', @rowcount_Update_Insert)

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
	END
END TRY

BEGIN CATCH
	SELECT ERROR_NUMBER() AS ErrorNumber, ERROR_MESSAGE() AS ErrorMessage;
END CATCH
GO