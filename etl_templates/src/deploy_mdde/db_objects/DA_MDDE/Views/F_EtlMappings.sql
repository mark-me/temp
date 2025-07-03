CREATE VIEW [DA_MDDE].[F_EtlMappings]
AS
SELECT dr.EtlRunKey
	, de.EntityKey
	, dm.MappingKey
	, StartDateKey = ds.DateKey
	, StartTimeKey = dst.TimeKey
	, DltSeconds = datediff(Second, e.[StartLoadDateTime], e.[EndLoadDateTime])
	, DltMinute = datediff(Minute, e.[StartLoadDateTime], e.[EndLoadDateTime])
	, TroughPut = CASE 
		WHEN isnull([RowCountInsert], 0) = 0
			THEN 0
		ELSE (100000 / [RowCountInsert]) * (datediff(Second, e.[StartLoadDateTime], e.[EndLoadDateTime]))
		END
		, TroughPutRowPerSecond = CASE 
		WHEN isnull([RowCountInsert], 0) = 0
			THEN 0
		ELSE [RowCountInsert] / (datediff(Second, e.[StartLoadDateTime], e.[EndLoadDateTime]))
		END
	, e.[RowCountInsert]
	, e.[RowCountUpdate]
	, e.[RowCountDelete]
	, LoadSucces = case when e.[LoadOutcome] = 'Success' then 1 else 0 end
	, LoadFail = case when e.[LoadOutcome] <> 'Success' then 1 else 0 end
FROM [DA_MDDE].[ConfigExecutions] AS e
LEFT JOIN [DA_MDDE].[D_EtlRun] dr ON e.[LoadRunId] = dr.[LoadRunId]
LEFT JOIN [DA_MDDE].[D_Entity] de ON e.[TargetName] = de.[EntityName]
LEFT JOIN [DA_MDDE].[D_Mapping] dm ON e.[MappingName] = dm.[MappingName]
LEFT JOIN [DA_MDDE].[D_Date] ds ON CAST(e.[StartLoadDateTime] AS DATE) = ds.[Date]
LEFT JOIN [DA_MDDE].[D_Time] dst ON CONVERT(VARCHAR, e.[StartLoadDateTime], 108) = dst.[Time]
