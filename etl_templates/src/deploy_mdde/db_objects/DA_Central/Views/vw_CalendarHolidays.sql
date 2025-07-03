CREATE VIEW [DA_Central].[vw_CalendarHolidays]
AS
SELECT [DateId] = CONVERT(NVARCHAR(255), [Date], 112)
	, [Date] = [Date]
	, [Code] = N'New Years Day'
	, [CodeNL] = N'Nieuwjaarsdag'
	, [DescriptionNL] = N'Nieuwjaarsdag'
	, [DescriptionEN] = N'New Year''s Day'
	, [IsHoliday] = 1
	, [IsPublicHoliday] = 1
	, [CountryRegionCode] = N'NL'
	, [CountryOrRegion] = N'Netherlands'
FROM [DA_MDDE].[Dates]
WHERE 1 = 1 AND DATENAME(dayofyear, [Date]) = 1

UNION ALL

/* King's Day Koning Willem-Alexander (2013 - ) */
SELECT [DateId] = CONVERT(NVARCHAR(255), [Date], 112)
	, [Date]
	, [Code] = N'Kings Day'
	, [CodeNL] = N'Koningsdag'
	, [DescriptionNL] = N'Koningsdag'
	, [DescriptionEN] = N'King''s Day'
	, [IsHoliday] = 1
	, [IsPublicHoliday] = 1
	, [CountryRegionCode] = N'NL'
	, [CountryOrRegion] = N'Netherlands'
FROM [DA_MDDE].[Dates]
WHERE 1 = 1 AND DATENAME(year, [Date]) >= '2014' AND DATENAME(month, [Date]) = 'April' AND ((DATENAME(day, [Date]) = 27 AND DATENAME(weekday, [Date]) <> 'Sunday') OR (DATENAME(day, [Date]) = 26 AND DATENAME(weekday, [Date]) = 'Saturday'))

UNION ALL

/* Queen's Day Queen Beatrix (1980 - 2013) */
SELECT [DateId] = CONVERT(NVARCHAR(255), [Date], 112)
	, [Date]
	, [Code] = N'Queens Day'
	, [CodeNL] = N'Koninginnedag'
	, [DescriptionNL] = N'Koninginnedag'
	, [DescriptionEN] = N'Queen''s Day'
	, [IsHoliday] = 1
	, [IsPublicHoliday] = 1
	, [CountryRegionCode] = N'NL'
	, [CountryOrRegion] = N'Netherlands'
FROM [DA_MDDE].[Dates]
WHERE 1 = 1 AND DATENAME(year, [Date]) >= '1980' AND DATENAME(year, [Date]) < '2014' AND ((DATENAME(month, [Date]) = 'April' AND DATENAME(day, [Date]) = 30 AND DATENAME(weekday, [Date]) <> 'Sunday') OR (DATENAME(month, [Date]) = 'April' AND DATENAME(day, [Date]) = 29 AND DATENAME(weekday, [Date]) = 'Saturday'))

UNION ALL

/* Queen's Day Queeen Juliana (1949 - 1980) */
SELECT [DateId] = CONVERT(NVARCHAR(255), [Date], 112)
	, [Date]
	, [Code] = N'Queens Day'
	, [CodeNL] = N'Koninginnedag'
	, [DescriptionNL] = N'Koninginnedag'
	, [DescriptionEN] = N'Queen''s Day'
	, [IsHoliday] = 1
	, [IsPublicHoliday] = 1
	, [CountryRegionCode] = N'NL'
	, [CountryOrRegion] = N'Netherlands'
FROM [DA_MDDE].[Dates]
WHERE 1 = 1 AND DATENAME(year, [Date]) >= '1949' AND DATENAME(year, [Date]) < '1980' AND ((DATENAME(month, [Date]) = 'April' AND DATENAME(day, [Date]) = 30 AND DATENAME(weekday, [Date]) <> 'Sunday') OR (DATENAME(month, [Date]) = 'May' AND DATENAME(day, [Date]) = 1 AND DATENAME(weekday, [Date]) = 'Monday'))

UNION ALL

/* Queen's Day Queeen Wilhelmina (1890 - 1949) */
SELECT [DateId] = CONVERT(NVARCHAR(255), [Date], 112)
	, [Date]
	, [Code] = N'Queens Day'
	, [CodeNL] = N'Koninginnedag'
	, [DescriptionNL] = N'Koninginnedag'
	, [DescriptionEN] = N'Queen''s Day'
	, [IsHoliday] = 1
	, [IsPublicHoliday] = 1
	, [CountryRegionCode] = N'NL'
	, [CountryOrRegion] = N'Netherlands'
FROM [DA_MDDE].[Dates]
WHERE 1 = 1 AND DATENAME(year, [Date]) >= '1891' AND DATENAME(year, [Date]) < '1949' AND DATENAME(month, [Date]) = 'August' AND DATENAME(day, [Date]) = 31

UNION ALL

/* Liberation Day */
SELECT [DateId] = CONVERT(NVARCHAR(255), [Date], 112)
	, [Date]
	, [Code] = N'Liberation Day'
	, [CodeNL] = N'Bevrijdingsdag'
	, [DescriptionNL] = N'Bevrijdingsdag'
	, [DescriptionEN] = N'Liberation Day'
	, [IsHoliday] = 1
	, [IsPublicHoliday] = CASE WHEN YEAR([Date]) >= 1990 THEN 1 ELSE 0 END
	, [CountryRegionCode] = N'NL'
	, [CountryOrRegion] = N'Netherlands'
FROM [DA_MDDE].[Dates]
WHERE 1 = 1 AND ((DATENAME(year, [Date]) = '1945' AND DATENAME(month, [Date]) = 'August' AND DATENAME(day, [Date]) = 31) OR (DATENAME(year, [Date]) > '1945' AND DATENAME(year, [Date]) < '1958' AND DATENAME(month, [Date]) = 'May' AND DATENAME(day, [Date]) = 5 AND DATENAME(weekday, [Date]) <> 'Sunday') OR (DATENAME(year, [Date]) >= '1958' AND DATENAME(year, [Date]) < '1990' AND YEAR([Date]) % 5 = 0 AND DATENAME(month, [Date]) = 'May' AND DATENAME(day, [Date]) = 5) OR (DATENAME(year, [Date]) >= '1990' AND DATENAME(month, [Date]) = 'May' AND DATENAME(day, [Date]) = 5))

UNION ALL

/* Easter Sunday */
SELECT [DateId] = CONVERT(NVARCHAR(255), [Date], 112)
	, [Date]
	, [Code] = N'Easter'
	, [CodeNL] = N'Paaszondag'
	, [DescriptionNL] = N'Paaszondag'
	, [DescriptionEN] = N'Easter Sunday'
	, [IsHoliday] = 1
	, [IsPublicHoliday] = 1
	, [CountryRegionCode] = N'NL'
	, [CountryOrRegion] = N'Netherlands'
FROM [DA_MDDE].[Dates]
WHERE 1 = 1 AND [DA_Central].[fn_CheckDateIsEasterDate]([Date]) = 1

UNION ALL

/* Easter Monday */
SELECT [DateId] = CONVERT(NVARCHAR(255), DATEADD(day, 1, [Date]), 112)
	, [Date] = DATEADD(day, 1, [Date])
	, [Code] = N'Easter'
	, [CodeNL] = N'Paasmaandag'
	, [DescriptionNL] = N'Paasmaandag'
	, [DescriptionEN] = N'Easter Monday'
	, [IsHoliday] = 1
	, [IsPublicHoliday] = 1
	, [CountryRegionCode] = N'NL'
	, [CountryOrRegion] = N'Netherlands'
FROM [DA_MDDE].[Dates]
WHERE 1 = 1 AND [DA_Central].[fn_CheckDateIsEasterDate]([Date]) = 1

UNION ALL

/* 	Good Friday */
SELECT [DateId] = CONVERT(NVARCHAR(255), DATEADD(day, - 2, [Date]), 112)
	, [Date] = DATEADD(day, - 2, [Date])
	, [Code] = N'Good Friday'
	, [CodeNL] = N'Goede vrijdag'
	, [DescriptionNL] = N'Goede vrijdag'
	, [DescriptionEN] = N'Good Friday'
	, [IsHoliday] = 1
	, [IsPublicHoliday] = 0
	, [CountryRegionCode] = N'NL'
	, [CountryOrRegion] = N'Netherlands'
FROM [DA_MDDE].[Dates]
WHERE 1 = 1 AND [DA_Central].[fn_CheckDateIsEasterDate]([Date]) = 1

UNION ALL

/* Ascension day */
SELECT [DateId] = CONVERT(NVARCHAR(255), DATEADD(day, 39, [Date]), 112)
	, [Date] = DATEADD(day, 39, [Date])
	, [Code] = N'Ascension'
	, [CodeNL] = N'hemelvaarts dag'
	, [DescriptionNL] = N'hemelvaarts dag'
	, [DescriptionEN] = N'Ascension day'
	, [IsHoliday] = 1
	, [IsPublicHoliday] = 1
	, [CountryRegionCode] = N'NL'
	, [CountryOrRegion] = N'Netherlands'
FROM [DA_MDDE].[Dates]
WHERE 1 = 1 AND [DA_Central].[fn_CheckDateIsEasterDate]([Date]) = 1

UNION ALL

/* Pentecost */
SELECT [DateId] = CONVERT(NVARCHAR(255), DATEADD(day, 49, [Date]), 112)
	, [Date] = DATEADD(day, 49, [Date])
	, [Code] = N'Pentecost'
	, [CodeNL] = N'Pinksteren'
	, [DescriptionNL] = N'Pinksterzondag'
	, [DescriptionEN] = N'Pentecost Sunday'
	, [IsHoliday] = 1
	, [IsPublicHoliday] = 1
	, [CountryRegionCode] = N'NL'
	, [CountryOrRegion] = N'Netherlands'
FROM [DA_MDDE].[Dates]
WHERE 1 = 1 AND [DA_Central].[fn_CheckDateIsEasterDate]([Date]) = 1

UNION ALL

/* Pentecost */
SELECT [DateId] = CONVERT(NVARCHAR(255), DATEADD(day, 50, [Date]), 112)
	, [Date] = DATEADD(day, 50, [Date])
	, [Code] = N'Pentecost'
	, [CodeNL] = N'Pinksteren'
	, [DescriptionNL] = N'Pinkstermaandag'
	, [DescriptionEN] = N'Pentecost Monday'
	, [IsHoliday] = 1
	, [IsPublicHoliday] = 1
	, [CountryRegionCode] = N'NL'
	, [CountryOrRegion] = N'Netherlands'
FROM [DA_MDDE].[Dates]
WHERE 1 = 1 AND [DA_Central].[fn_CheckDateIsEasterDate]([Date]) = 1

UNION ALL

/* Christmas */
SELECT [DateId] = CONVERT(NVARCHAR(255), [Date], 112)
	, [Date] = [Date]
	, [Code] = N'Christmas'
	, [CodeNL] = N'Kerstmis'
	, [DescriptionNL] = N'Eerste kerstdag'
	, [DescriptionEN] = N'Christmas Day'
	, [IsHoliday] = 1
	, [IsPublicHoliday] = 1
	, [CountryRegionCode] = N'NL'
	, [CountryOrRegion] = N'Netherlands'
FROM [DA_MDDE].[Dates]
WHERE 1 = 1 AND (DATENAME(month, [Date]) = 'December' AND DATENAME(day, [Date]) = 25)

UNION ALL

/* Christmas */
SELECT [DateId] = CONVERT(NVARCHAR(255), [Date], 112)
	, [Date] = [Date]
	, [Code] = N'Christmas'
	, [CodeNL] = N'Kerstmis'
	, [DescriptionNL] = N'Tweede kerstdag'
	, [DescriptionEN] = N'Boxing Day'
	, [IsHoliday] = 1
	, [IsPublicHoliday] = 1
	, [CountryRegionCode] = N'NL'
	, [CountryOrRegion] = N'Netherlands'
FROM [DA_MDDE].[Dates]
WHERE 1 = 1 AND (DATENAME(month, [Date]) = 'December' AND DATENAME(day, [Date]) = 26)

UNION ALL

/* Saint Nicholas Day */
SELECT [DateId] = CONVERT(NVARCHAR(255), [Date], 112)
	, [Date]
	, [Code] = N'Saint Nicholas'
	, [CodeNL] = N'Sinterklaas'
	, [DescriptionNL] = N'Sinterklaas'
	, [DescriptionEN] = N'Saint Nicholas'
	, [IsHoliday] = 1
	, [IsPublicHoliday] = 0
	, [CountryRegionCode] = N'NL'
	, [CountryOrRegion] = N'Netherlands'
FROM [DA_MDDE].[Dates]
WHERE 1 = 1 AND (DATENAME(month, [Date]) = 'December' AND DATENAME(day, [Date]) = 5)

UNION ALL

/* Remembrance Day */
SELECT [DateId] = CONVERT(NVARCHAR(255), [Date], 112)
	, [Date]
	, [Code] = N'Remembrance Day'
	, [CodeNL] = N'Dodenherdenking'
	, [DescriptionNL] = N'Dodenherdenking'
	, [DescriptionEN] = N'Remembrance Day'
	, [IsHoliday] = 1
	, [IsPublicHoliday] = 0
	, [CountryRegionCode] = N'NL'
	, [CountryOrRegion] = N'Netherlands'
FROM [DA_MDDE].[Dates]
WHERE 1 = 1 AND ((DATENAME(year, [Date]) = 1945 AND DATENAME(month, [Date]) = 'August' AND DATENAME(day, [Date]) = 31) OR (DATENAME(year, [Date]) > 1945 AND DATENAME(year, [Date]) < '1968' AND DATENAME(month, [Date]) = 'May' AND DATENAME(day, [Date]) = 4 AND DATENAME(weekday, [Date]) <> 'Sunday') OR (DATENAME(year, [Date]) > 1945 AND DATENAME(year, [Date]) < '1968' AND DATENAME(month, [Date]) = 'May' AND DATENAME(day, [Date]) = 3 AND DATENAME(weekday, [Date]) = 'Saturday') OR (DATENAME(year, [Date]) >= 1968 AND DATENAME(month, [Date]) = 'May' AND DATENAME(day, [Date]) = 4))

UNION ALL

/* New Year's Eve */
SELECT [DateId] = CONVERT(NVARCHAR(255), [Date], 112)
	, [Date]
	, [Code] = N'New Years Eve'
	, [CodeNL] = N'Eindejaarsavond'
	, [DescriptionNL] = N'Eindejaarsavond'
	, [DescriptionEN] = N'New Year''s Eve'
	, [IsHoliday] = 1
	, [IsPublicHoliday] = 0
	, [CountryRegionCode] = N'NL'
	, [CountryOrRegion] = N'Netherlands'
FROM [DA_MDDE].[Dates]
WHERE 1 = 1 AND (DATENAME(month, [Date]) = 'December' AND DATENAME(day, [Date]) = 31);
