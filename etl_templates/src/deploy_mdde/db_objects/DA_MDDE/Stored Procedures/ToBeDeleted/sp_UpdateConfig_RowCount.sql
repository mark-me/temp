CREATE PROC [DA_MDDE].[sp_UpdateConfig_RowCount] @par_runid [NVARCHAR] (500)
	, @par_LayerName [NVARCHAR] (500)
	, @par_MappingName [NVARCHAR] (500)
	, @par_RowCountInsert [BigInt]
	, @par_RowCountUpdate [BigInt]
	, @par_RowCountDelete [BigInt]
	, @par_Debug [Bit]
AS
/***************************************************************************************************
Script Name         sp_UpdateConfig_ErrorPredecessor.sql
Create Date:        2025-07-10
Author:             Jeroen Poll
Description:        PROC
Used By:            -
Parameter(s):       - @par_runid [NVARCHAR] (500)  /* ETL RunID form the Synapse Pipeline. */
					, @par_LayerName [NVARCHAR] (500)  /* Schema Name. */
					, @par_MappingName [NVARCHAR] (500) /* Mapping name from PowerDesigner. Is only used for logging. */
					, @par_Debug [Boolean] /* If true, only statement will be printed and not executed. */

Usage:              -
****************************************************************************************************
SUMMARY OF CHANGES
Date(yyyy-mm-dd)    Author              Comments
------------------- ------------------- ------------------------------------------------------------
2025-07-10        Jeroen Poll         Initial Script V1.0   First version

***************************************************************************************************/
BEGIN
	UPDATE [DA_MDDE].[Config]
	SET [LoadRunId] = @par_runid
		, [LoadEndDateTime] = GETDATE() AT TIME ZONE 'UTC' AT TIME ZONE 'W. Europe Standard Time'
		, [RowCountInsert] = @par_RowCountInsert
		, [RowCountUpdate] = @par_RowCountUpdate
		, [RowCountDelete] = @par_RowCountDelete
		, [LoadOutcome] = 'OK'
	FROM [DA_MDDE].[Config] AS conf
	WHERE 1 = 1 AND [LayerName] = @par_LayerName AND [MappingName] = @par_MappingName
END
GO


