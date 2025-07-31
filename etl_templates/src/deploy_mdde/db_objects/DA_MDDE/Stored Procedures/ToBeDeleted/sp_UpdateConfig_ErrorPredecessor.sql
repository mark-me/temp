CREATE PROC [DA_MDDE].[sp_UpdateConfig_ErrorPredecessor] @par_runid [NVARCHAR] (500)
	, @par_LayerName [NVARCHAR] (500)
	, @par_MappingName [NVARCHAR] (500)
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
2025-07-24		  Jeroen Poll		  Disabled for new release 
***************************************************************************************************/
BEGIN
	SELECT 1
	-- UPDATE [DA_MDDE].[Config]
	-- SET [LoadType] = 98
	-- 	, [LoadRunId] = @par_runid
	-- 	, [LoadStartDateTime] = GETDATE() AT TIME ZONE 'UTC' AT TIME ZONE 'W. Europe Standard Time'
	-- 	, [LoadEndDateTime] = GETDATE() AT TIME ZONE 'UTC' AT TIME ZONE 'W. Europe Standard Time'
	-- 	, [LoadOutcome] = 'Did Not Run'
	-- FROM [DA_MDDE].[Config] AS conf
	-- INNER JOIN [DA_MDDE].[LoadDependencies] AS dep ON dep.[ModelRelated] = conf.[LayerName] AND dep.[MappingRelated] = conf.[MappingName]
	-- WHERE 1 = 1 AND dep.[Model] = @par_LayerName AND dep.[Mapping] = @par_MappingName
END
GO


