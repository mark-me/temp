CREATE VIEW [DA_MDDE].[D_EtlRun]
AS
SELECT 
EtlRunKey = ROW_NUMBER() OVER ( ORDER BY StartLoad)
,[LoadRunId]
,[StartLoad]
,[Outcome]
FROM(
SELECT  [LoadRunId]
	, [StartLoad] = CAST(MIN([StartLoadDateTime]) AS DATETIME)
	, [Outcome] = MAX(CASE 
			WHEN [LoadOutcome] = 'Success'
				THEN 'Success'
			ELSE 'Fail'
			END)
FROM [DA_MDDE].[ConfigExecutions]
GROUP BY [LoadRunId]) a

