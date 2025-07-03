CREATE PROC [DA_MDDE].[sp_PrintLoadingDatatypeCheck] @par_runid [NVARCHAR](500),@par_LayerName [NVARCHAR](500),@par_SourceName [NVARCHAR](500),@par_DestinationName [NVARCHAR](500) AS
/***************************************************************************************************
Script Name         sp_PrintLoadingDatatypeCheck.sql
Create Date:        2025-04-04
Author:             Jeroen Poll
Description:        PROC
Used By:            -
Parameter(s):       - @par_runid [NVARCHAR] (500)  /* ETL RunID form the Synapse Pipeline. */
					, @par_LayerName [NVARCHAR] (500)  /* Schema Name. */
					, @par_SourceName [NVARCHAR] (500) /* Source table or view Name. */
					, @par_DestinationName [NVARCHAR] (500) /* Desitination table or view Name. */
Usage:              -
****************************************************************************************************
SUMMARY OF CHANGES
Date(yyyy-mm-dd)    Author              Comments
------------------- ------------------- ------------------------------------------------------------
2025-04-04	        Jeroen Poll         Initial Script V1.0   First version

***************************************************************************************************/
BEGIN TRY
	DECLARE @LogMessage NVARCHAR(MAX);

	SET @LogMessage = (
			SELECT STRING_AGG(print_message, '')
			FROM (
				SELECT CONCAT ('Loading for table ', CONCAT (QUOTENAME(@par_LayerName), '.', QUOTENAME(@par_DestinationName)), ' has datatype issue with loading of source: ', CONCAT (QUOTENAME(@par_LayerName), '.', QUOTENAME(@par_SourceName)), CHAR(13) + CHAR(10)) AS print_message
				
				UNION ALL
				
				SELECT CONCAT ('There is type or length mismatch:', CHAR(13) + CHAR(10)) AS print_message
				
				UNION ALL
				
				SELECT CONCAT ('--------------------------------------------------------------------------------------------------------------------------------------------------', CHAR(13) + CHAR(10)) AS print_message
				
				UNION ALL
				
				SELECT CONCAT ('|', ' Columnname' + SUBSTRING('                                                   |', LEN(' Columnname'), 53 - LEN(' Columnname')), ' SourceType' + SUBSTRING('                     |', LEN(' SourceType'), 23 - LEN(' SourceType')), ' DestinationType' + SUBSTRING('                     |', LEN(' DestinationType'), 23 - LEN(' DestinationType')), ' SourceLength' + SUBSTRING('                     |', LEN(' SourceLength'), 23 - LEN(' SourceLength')), ' DestinationLength' + SUBSTRING('                     |', LEN(' DestinationLength'), 23 - LEN(' DestinationLength')), CHAR(13) + CHAR(10)) AS print_message
				
				UNION ALL
				
				SELECT CONCAT ('--------------------------------------------------------------------------------------------------------------------------------------------------', CHAR(13) + CHAR(10)) AS print_message
				
				UNION ALL
				
				SELECT CONCAT ('|', ' ' + CAST(source.[name] AS NVARCHAR(50)) + SUBSTRING('                                                   |', LEN(' ' + CAST(source.[name] AS NVARCHAR(50))), 53 - LEN(' ' + CAST(source.[name] AS NVARCHAR(50)))), ' ' + CAST(sourcetype.[name] AS NVARCHAR(50)) + SUBSTRING('                     |', LEN(' ' + CAST(sourcetype.[name] AS NVARCHAR(50))), 23 - LEN(' ' + CAST(sourcetype.[name] AS NVARCHAR(50)))), ' ' + CAST(desttype.[name] AS NVARCHAR(50)) + SUBSTRING('                     |', LEN(' ' + CAST(desttype.[name] AS NVARCHAR(50))), 23 - LEN(' ' + CAST(desttype.[name] AS NVARCHAR(50)))), ' ' + CAST(source.[max_length] AS NVARCHAR(50)) + SUBSTRING('                     |', LEN(' ' + CAST(source.[max_length] AS NVARCHAR(50))), 23 - LEN(' ' + CAST(source.[max_length] AS NVARCHAR(50)))), ' ' + CAST(dest.[max_length] AS NVARCHAR(50)) + SUBSTRING('                     |', LEN(' ' + CAST(dest.[max_length] AS NVARCHAR(50))), 23 - LEN(' ' + CAST(dest.[max_length] AS NVARCHAR(50)))), CHAR(13) + CHAR(10))
				FROM sys.columns dest
				LEFT JOIN sys.columns source ON source.object_id = OBJECT_ID(CONCAT (QUOTENAME(@par_LayerName), '.', QUOTENAME(@par_SourceName))) AND dest.[name] = source.[name]
				LEFT JOIN sys.types sourcetype ON sourcetype.system_type_id = sourcetype.user_type_id AND sourcetype.system_type_id = source.system_type_id
				LEFT JOIN sys.types desttype ON desttype.system_type_id = desttype.user_type_id AND desttype.system_type_id = dest.system_type_id
				WHERE dest.object_id = OBJECT_ID(CONCAT (QUOTENAME(@par_LayerName), '.', QUOTENAME(@par_DestinationName))) AND dest.is_identity = 0 AND (source.system_type_id <> dest.system_type_id OR source.max_length <> dest.max_length)
				
				UNION ALL
				
				SELECT CONCAT ('--------------------------------------------------------------------------------------------------------------------------------------------------', CHAR(13) + CHAR(10)) AS print_message
				) a
			)
	EXEC [DA_MDDE].[sp_Logger] 'WARNING', @LogMessage
END TRY

BEGIN CATCH
	SELECT ERROR_NUMBER() AS ErrorNumber, ERROR_MESSAGE() AS ErrorMessage;
END CATCH
GO


