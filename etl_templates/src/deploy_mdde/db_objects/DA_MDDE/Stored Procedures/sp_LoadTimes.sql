CREATE PROC [DA_MDDE].[sp_LoadTimes]
AS
TRUNCATE TABLE [DM_Dim].[Time];

SET IDENTITY_INSERT [DM_Dim].[Time] ON

INSERT INTO [DM_Dim].[Time] (
	[TimeKey]
	, [Time]
	, [Hour]
	, [MilitaryHour]
	, [Minute]
	, [Second]
	, [AmPm]
	, [X_StartDate]
	, [X_EndDate]
	, [X_HashKey]
	, [X_IsCurrent]
	, [X_IsReplaced]
	, [X_RunId]
	, [X_LoadDateTime]
	, [X_Bron]
	)
SELECT CAST(REPLACE(CONVERT(VARCHAR, a.[Tijd], 108), ':', '') AS INT) [TimeKey]
	, CONVERT(VARCHAR, a.[Tijd], 108) [Time]
	, CASE 
		WHEN DATEPART(HOUR, a.[Tijd]) > 12
			THEN DATEPART(HOUR, a.[Tijd]) - 12
		ELSE DATEPART(HOUR, a.[Tijd])
		END AS [Hour]
	, CAST(SUBSTRING(CONVERT(VARCHAR, a.[Tijd], 108), 1, 2) AS INT) [MilitaryHour]
	, DATEPART(MINUTE, a.[Tijd]) [Minute]
	, DATEPART(SECOND, a.[Tijd]) [Second]
	, CASE 
		WHEN DATEPART(HOUR, a.[Tijd]) >= 12
			THEN 'PM'
		ELSE 'AM'
		END AS [AmPm]
	, [X_StartDate] = '1900-01-01'
	, [X_EndDate] = '2099-12-31'
	, [X_HashKey] = 0
	, [X_IsCurrent] = 1
	, [X_IsReplaced] = 0
	, [X_RunId] = '0000'
	, [X_LoadDateTime] = GETDATE()
	, [X_Bron] = 'Calendar'
FROM (
	SELECT TOP 86400 /* needs a top x to prevent a overflow of the date datatype */
		DATEADD(second, (
				ROW_NUMBER() OVER (
					ORDER BY tijd.object_id
						, tijdrange.object_id ASC
					)
				), CAST('00:00:00' AS TIME)) AS Tijd
	FROM sys.objects AS tijd
	CROSS JOIN sys.objects AS tijdrange
	) a
WHERE a.[Tijd] BETWEEN '00:00:00' AND '23:59:59'
GROUP BY a.[Tijd];

UPDATE [DM_Dim].[Time]
SET [HOUR] = '0' + [HOUR]
WHERE LEN([HOUR]) = 1

UPDATE [DM_Dim].[Time]
SET [MINUTE] = '0' + [MINUTE]
WHERE LEN([MINUTE]) = 1

UPDATE [DM_Dim].[Time]
SET [SECOND] = '0' + [SECOND]
WHERE LEN([SECOND]) = 1

UPDATE [DM_Dim].[Time]
SET StandardTime = [Hour] + ':' + [Minute] + ':' + [Second] + ' ' + AmPm
WHERE StandardTime IS NULL AND HOUR <> '00'

UPDATE [DM_Dim].[Time]
SET StandardTime = '12' + ':' + [Minute] + ':' + [Second] + ' ' + AmPm
WHERE [HOUR] = '00'
GO


