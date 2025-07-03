CREATE VIEW [DA_MDDE].[F_EtlRuns]
AS
SELECT dr.EtlRunKey
	, StartDateKey = MIN(ds.DateKey)
	, StartTimeKey = MIN(dst.TimeKey)
	, DltSeconds = datediff(Second, MIN(e.[StartLoadDateTime]), MAX(e.[EndLoadDateTime]))
	, DltMinute = datediff(Minute, MIN(e.[StartLoadDateTime]), MAX(e.[EndLoadDateTime]))
	, TroughPutRowPerSecond = CASE 
		WHEN isnull(SUM([RowCountInsert]), 0) = 0
			THEN 0
		ELSE sum([RowCountInsert]) / (datediff(Second, MIN(e.[StartLoadDateTime]), MAX(e.[EndLoadDateTime])))
		END
	, [RowCountInsert] = SUM(e.[RowCountInsert])
	, [RowCountUpdate] = SUM(e.[RowCountUpdate])
	, [RowCountDelete] = SUM(e.[RowCountDelete])
	, EntitiesLoaded = COUNT(DISTINCT e.[TargetName])
	, MappingsLoaded = COUNT(DISTINCT e.[MappingName])
	, LoadSucces = MIN(CASE 
			WHEN e.[LoadOutcome] = 'Success'
				THEN 1
			ELSE 0
			END)
	, LoadFail = MAX(CASE 
			WHEN e.[LoadOutcome] <> 'Success'
				THEN 1
			ELSE 0
			END)
FROM [DA_MDDE].[ConfigExecutions] AS e
LEFT JOIN [DA_MDDE].[D_EtlRun] dr ON e.[LoadRunId] = dr.[LoadRunId]
--LEFT JOIN [DA_MDDE].[D_Entity] de ON e.[TargetName] = de.[EntityName]
--LEFT JOIN [DA_MDDE].[D_Mapping] dm ON e.[MappingName] = dm.[MappingName]
LEFT JOIN [DA_MDDE].[D_Date] ds ON CAST(e.[StartLoadDateTime] AS DATE) = ds.[Date]
LEFT JOIN [DA_MDDE].[D_Time] dst ON CONVERT(VARCHAR, e.[StartLoadDateTime], 108) = dst.[Time]
GROUP BY dr.EtlRunKey

