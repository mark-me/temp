TRUNCATE TABLE [DA_Central].[Calendar];
go

TRUNCATE TABLE [DM_Dim].[Date];
go

INSERT INTO  [DA_Central].[Calendar]
SELECT * FROM [DA_Central].[vw_src_Calendar];
go



-- SET IDENTITY_INSERT to ON.
SET IDENTITY_INSERT [DM_Dim].[Date] ON;



INSERT INTO [DM_Dim].[Date] (
	[DateKey]
	, [Date]
	, [DayMonthNo]
	, [DayQuarterNo]
	, [DayYearNo]
	, [StartWeekDate]
	, [StartMonthDate]
	, [StartQuarterDate]
	, [StartYearDate]
	, [EndWeekDate]
	, [EndMontDate]
	, [EndQuarterDate]
	, [EndYearDate]
	, [IsLeapYear]
	, [IsWeekend]
	, [YearNo]
	, [YearNoIso]
	, [YearNoDou]
	, [QuarterNo]
	, [MonthNo]
	, [YearQuarterDesc]
	, [YearMonthDesc]
	, [YearWeekDesc]
	, [YearWeekDescIso]
	, [YearWeekDescDou]
	, [MonthNameEN]
	, [MonthNameNL]
	, [MonthNameShortEN]
	, [MonthNameShortNL]
	, [WeekNo]
	, [WeekNoIso]
	, [WeekNoDou]
	, [WeekDayNoEN]
	, [WeekDayNoNL]
	, [WeekDayNameEN]
	, [WeekDayNameNL]
	, [WeekDayOfMonth]
	, [WeekDayNameShortEN]
	, [WeekDayNameShortNL]
	, [DateYesterday]
	, [DateLastWeek]
	, [DateLastMonth]
	, [DateLastYear]
	, [IsHoliday]
	, [IsPublicHoliday]
	, [X_StartDate]
	, [X_EndDate]
	, [X_HashKey]
	, [X_IsCurrent]
	, [X_IsReplaced]
	, [X_RunId]
	, [X_LoadDateTime]
	, [X_Bron]
	)
SELECT DateKey = CAST(FORMAT(Calendar.[Calendar_Date], 'yyyyMMdd') AS INT)
	, DATE = Calendar.[Calendar_Date]
	, DayMonthNo = DATEPART(day, Calendar.[Calendar_Date])
	, DayQuarterNo = DATEDIFF(DD, CONVERT(DATE, DATEADD(QQ, DATEDIFF(QQ, 0, Calendar.[Calendar_Date]), 0)), Calendar.[Calendar_Date]) + 1
	, DayYearNo = DATEPART(DAYOFYEAR, Calendar.[Calendar_Date])
	, StartWeekDate = CAST(DATEADD(WW, DATEDIFF(WW, 0, Calendar.[Calendar_Date]), 0) AS DATE)
	, StartMonthDate = CAST(DATEADD(MONTH, DATEDIFF(MONTH, 0, Calendar.[Calendar_Date]), 0) AS DATE)
	, StartQuarterDate = CAST(DATEADD(QUARTER, DATEDIFF(QUARTER, 0, Calendar.[Calendar_Date]), 0) AS DATE)
	, StartYearDate = CAST(DATEADD(YEAR, DATEDIFF(YEAR, 0, Calendar.[Calendar_Date]), 0) AS DATE)
	, EndWeekDate = DATEADD(DAY, 6, CAST(DATEADD(WEEK, DATEDIFF(WEEK, 0, Calendar.[Calendar_Date]), 0) AS DATE))
	, EndMontDate = EOMONTH(Calendar.[Calendar_Date])
	, EndQuarterDate = CAST(DATEADD(DD, - 1, DATEADD(QQ, DATEDIFF(QQ, 0, Calendar.[Calendar_Date]) + 1, 0)) AS DATE)
	, EndYearDate = CAST(DATEADD(DD, - 1, DATEADD(YY, DATEDIFF(YY, 0, Calendar.[Calendar_Date]) + 1, 0)) AS DATE)
	, IsLeapYear = CASE WHEN DATEPART(YEAR, Calendar.[Calendar_Date]) % 4 = 0 THEN 1 ELSE 0 END
	, IsWeekend = CASE WHEN DATEPART(WEEKDAY, Calendar.[Calendar_Date]) IN (1, 7) THEN 1 ELSE 0 END
	, YearNo = DATEPART(YEAR, Calendar.[Calendar_Date])
	, YearNoIso = YEAR(DATEADD(DAY, 26 - DATEPART(isoww, Calendar.[Calendar_Date]), Calendar.[Calendar_Date]))
	, YearNoDou = DATEPART(YEAR, Calendar.[Calendar_Date])
	, QuarterNo = DATEPART(QQ, Calendar.[Calendar_Date])
	, MonthNo = DATEPART(MM, Calendar.[Calendar_Date])
	, YearQuarterDesc = CAST(DATEPART(year, Calendar.[Calendar_Date]) AS VARCHAR(4)) + '_' + FORMAT(DATEPART(quarter, Calendar.[Calendar_Date]), '00')
	, YearMonthDesc = CAST(DATEPART(year, Calendar.[Calendar_Date]) AS VARCHAR(4)) + '_' + FORMAT(DATEPART(month, Calendar.[Calendar_Date]), '00')
	, YearWeekDesc = CONCAT (
		YEAR(Calendar.[Calendar_Date])
		, '_'
		, FORMAT(DATEPART(WEEK, Calendar.[Calendar_Date]), '0#')
		)
	, YearWeekDescIso = CONCAT (
		YEAR(DATEADD(DAY, 26 - DATEPART(ISO_WEEK, Calendar.[Calendar_Date]), Calendar.[Calendar_Date]))
		, '_'
		, FORMAT(DATEPART(ISO_WEEK, Calendar.[Calendar_Date]), '0#')
		)
	, YearWeekDescDou = CONCAT (
		CAST(DATEPART(YEAR, Calendar.[Calendar_Date]) AS VARCHAR(4))
		, '_'
		, CASE WHEN DATEPART(MONTH, Calendar.[Calendar_Date]) = 12 AND DATEPART(ISO_WEEK, Calendar.[Calendar_Date]) IN (1, 53) THEN '52' WHEN DATEPART(MONTH, Calendar.[Calendar_Date]) = 1 AND DATEPART(ISO_WEEK, Calendar.[Calendar_Date]) > 51 THEN '01' ELSE FORMAT(DATEPART(ISO_WEEK, Calendar.[Calendar_Date]), '00') END
		)
	, MonthNameEN = FORMAT(Calendar.[Calendar_Date], 'MMMM', 'en-EN')
	, MonthNameNL = FORMAT(Calendar.[Calendar_Date], 'MMMM', 'nl-NL')
	, MonthNameShortEN = FORMAT(Calendar.[Calendar_Date], 'MMM', 'en-EN')
	, MonthNameShortNL = FORMAT(Calendar.[Calendar_Date], 'MMM', 'nl-NL')
	, WeekNo = DATEPART(WEEK, Calendar.[Calendar_Date])
	, WeekNoIso = DATEPART(ISO_WEEK, Calendar.[Calendar_Date])
	, WeekNoDou = CASE WHEN DATEPART(MONTH, Calendar.[Calendar_Date]) = 12 AND DATEPART(ISO_WEEK, Calendar.[Calendar_Date]) IN (1, 53) THEN '52' WHEN DATEPART(MONTH, Calendar.[Calendar_Date]) = 1 AND DATEPART(ISO_WEEK, Calendar.[Calendar_Date]) > 51 THEN '1' ELSE DATEPART(ISO_WEEK, Calendar.[Calendar_Date]) END
	, WeekDayNoEN = DATEPART(WEEKDAY, Calendar.[Calendar_Date]) 
	, WeekDayNoNL = CASE WHEN DATEPART(WEEKDAY, Calendar.[Calendar_Date]) = 1 THEN 7 ELSE DATEPART(WEEKDAY, Calendar.[Calendar_Date]) - 1 END
	, WeekDayNameEN = FORMAT(Calendar.[Calendar_Date], 'dddd', 'en-EN')
	, WeekDayNameNL = FORMAT(Calendar.[Calendar_Date], 'dddd', 'nl-NL')
	, WeekDayOfMonth = CEILING(DATEPART(day, Calendar.[Calendar_Date]) / 7.0)
	, WeekDayNameShortEN = FORMAT(Calendar.[Calendar_Date], 'ddd', 'en-EN')
	, WeekDayNameShortNL = FORMAT(Calendar.[Calendar_Date], 'ddd', 'nl-NL')
	, DateYesterday = DATEADD(DAY, - 1, Calendar.[Calendar_Date])
	, DateLastWeek = DATEADD(DAY, - 7, Calendar.[Calendar_Date])
	, DateLastMonth = DATEADD(MONTH, - 1, Calendar.[Calendar_Date])
	, DateLastYear = DATEADD(YEAR, - 1, Calendar.[Calendar_Date])
	, IsHoliday = Calendar.[IsHoliday]
	, IsPublicHoliday = Calendar.[IsPublicHoliday]
	, [X_StartDate] = CAST(GETDATE() AS DATE)
	, [X_EndDate] = '2099-12-31'
	, [X_HashKey] = HASHBYTES('SHA2_512', CONCAT (
			''
			, CAST(FORMAT(Calendar.[Calendar_Date], 'yyyyMMdd') AS NVARCHAR(255))
			))
	, [X_IsCurrent] = 1
	, [X_IsReplaced] = 0
	, [X_RunId] = 999
	, [X_LoadDateTime] = getdate()
	, [X_Bron] = 'Calendar'
FROM [DA_Central].[Calendar]


-- SET IDENTITY_INSERT to OFF.
SET IDENTITY_INSERT [DM_Dim].[Date] OFF;
go