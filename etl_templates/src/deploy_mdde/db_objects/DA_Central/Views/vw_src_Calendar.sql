CREATE VIEW [DA_Central].[vw_src_Calendar]
AS
SELECT a.[Date]
	,MAX(ISNULL([IsHoliday], 0)) AS [IsHoliday]
	,MAX(ISNULL([IsPublicHoliday], 0)) AS [IsPublicHoliday]
FROM (
	SELECT [Date]
	FROM [DA_MDDE].[Dates]
	) a
LEFT JOIN [DA_Central].[vw_CalendarHolidays] AS CalendarHolidaysView ON a.[Date] = CalendarHolidaysView.[Date]
	AND CalendarHolidaysView.[CountryRegionCode] = 'NL'
GROUP BY a.[Date]
