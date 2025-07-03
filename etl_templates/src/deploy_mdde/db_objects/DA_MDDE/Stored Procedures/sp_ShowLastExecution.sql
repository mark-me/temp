CREATE PROC DA_MDDE.sp_ShowLastExecution
AS
SELECT [ModelName]
	, [LayerName]
	, [MappingName]
	, [TargetName]
	, [SourceName]
	, [RunLevel]
	, [RunLevelStage]
	, [LoadType]
	, [StartLoadDateTime]
	, [EndLoadDateTime]
	, DATEDIFF(minute, [StartLoadDateTime], [EndLoadDateTime]) AS dlt
	, [RowCountInsert]
	, [RowCountUpdate]
	, [RowCountDelete]
	, [LoadOutcome]
FROM [DA_MDDE].[ConfigExecutions]
WHERE [LoadRunId] = (
		SELECT TOP 1 [LoadRunId]
		FROM [DA_MDDE].[ConfigExecutions]
		ORDER BY [StartLoadDateTime] DESC
		)
ORDER BY [StartLoadDateTime]
