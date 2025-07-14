CREATE PROC [DA_MDDE].[sp_UpdateConfig_ToDefaults]
AS
/***************************************************************************************************
Script Name         sp_UpdateConfig_ErrorPredecessor.sql
Create Date:        2025-07-10
Author:             Jeroen Poll
Description:        PROC
Used By:            -
Parameter(s):       

Usage:              -
****************************************************************************************************
SUMMARY OF CHANGES
Date(yyyy-mm-dd)    Author              Comments
------------------- ------------------- ------------------------------------------------------------
2025-07-10        Jeroen Poll         Initial Script V1.0   First version

***************************************************************************************************/
BEGIN
	/* Copy Config table to ConfigExecutions table */
	WITH maxLoadRunId
	AS (
		SELECT [MaxLoadRunId] = ISNULL(MAX([LoadRunId]), '00000000-0000-0000-0000-000000000000')
		FROM [DA_MDDE].[Config]
		)
	INSERT INTO [DA_MDDE].[ConfigExecutions] (
		[DateTime]
		, [ConfigKey]
		, [ModelName]
		, [LayerName]
		, [MappingName]
		, [TargetName]
		, [SourceName]
		, [RunLevel]
		, [RunLevelStage]
		, [LoadTypeDefault]
		, [LoadType]
		, [LoadCommand]
		, [LoadRunId]
		, [LoadStartDateTime]
		, [LoadEndDateTime]
		, [RowCountInsert]
		, [RowCountUpdate]
		, [RowCountDelete]
		, [LoadOutcome]
		)
	SELECT [DateTime] = GETDATE() AT TIME ZONE 'UTC' AT TIME ZONE 'W. Europe Standard Time'
		, [ConfigKey]
		, [ModelName]
		, [LayerName]
		, [MappingName]
		, [TargetName]
		, [SourceName]
		, [RunLevel]
		, [RunLevelStage]
		, [LoadTypeDefault]
		, [LoadType]
		, [LoadCommand]
		, [LoadRunId] = maxLoadRunId.MaxLoadRunId
		, [LoadStartDateTime]
		, [LoadEndDateTime]
		, [RowCountInsert]
		, [RowCountUpdate]
		, [RowCountDelete]
		, [LoadOutcome]
	FROM [DA_MDDE].[Config]
	INNER JOIN maxLoadRunId ON 1 = 1
	WHERE maxLoadRunId.MaxLoadRunId <> '00000000-0000-0000-0000-000000000000'
END

BEGIN
	/* Set Config table to defaults */
	UPDATE [DA_MDDE].[Config]
	SET [LoadType] = [LoadTypeDefault]
		, [LoadRunId] = NULL
		, [LoadStartDateTime] = NULL
		, [LoadEndDateTime] = NULL
		, [RowCountInsert] = NULL
		, [RowCountUpdate] = NULL
		, [RowCountDelete] = NULL
		, [LoadOutcome] = NULL
END
GO