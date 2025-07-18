﻿CREATE TABLE [DM_Dim].[Date]
(
	[DateKey] [int] IDENTITY(1,1) NOT NULL,
	[Date] [date] NULL,
	[DayMonthNo] [int] NULL,
	[DayQuarterNo] [int] NULL,
	[DayYearNo] [int] NULL,
	[StartWeekDate] [date] NULL,
	[StartMonthDate] [date] NULL,
	[StartQuarterDate] [date] NULL,
	[StartYearDate] [date] NULL,
	[EndWeekDate] [date] NULL,
	[EndMontDate] [date] NULL,
	[EndQuarterDate] [date] NULL,
	[EndYearDate] [date] NULL,
	[IsLeapYear] [int] NULL,
	[IsWeekend] [int] NULL,
	[YearNo] [int] NULL,
	[YearNoIso] [int] NULL,
	[YearNoDou] [int] NULL,
	[QuarterNo] [int] NULL,
	[MonthNo] [int] NULL,
	[YearQuarterDesc] [nvarchar](250) NULL,
	[YearMonthDesc] [nvarchar](250) NULL,
	[YearWeekDesc] [nvarchar](250) NULL,
	[YearWeekDescIso] [nvarchar](250) NULL,
	[YearWeekDescDou] [nvarchar](250) NULL,
	[MonthNameEN] [nvarchar](250) NULL,
	[MonthNameNL] [nvarchar](250) NULL,
	[MonthNameShortEN] [nvarchar](250) NULL,
	[MonthNameShortNL] [nvarchar](250) NULL,
	[WeekNo] [int] NULL,
	[WeekNoIso] [int] NULL,
	[WeekNoDou] [int] NULL,
	[WeekDayNoEN] [int] NULL,
	[WeekDayNoNL] [int] NULL,
	[WeekDayNameEN] [nvarchar](250) NULL,
	[WeekDayNameNL] [nvarchar](250) NULL,
	[WeekDayOfMonth] [int] NULL,
	[WeekDayNameShortEN] [nvarchar](250) NULL,
	[WeekDayNameShortNL] [nvarchar](250) NULL,
	[DateYesterday] [date] NULL,
	[DateLastWeek] [date] NULL,
	[DateLastMonth] [date] NULL,
	[DateLastYear] [date] NULL,
	[IsHoliday] [int] NULL,
	[IsPublicHoliday] [int] NULL,
	[X_StartDate] [date] NULL,
	[X_EndDate] [date] NULL,
	[X_HashKey] [varbinary](8000) NULL,
	[X_IsCurrent] [bit] NULL,
	[X_IsReplaced] [bit] NULL,
	[X_RunId] [int] NULL,
	[X_LoadDateTime] [datetime] NULL,
	[X_Bron] [nvarchar](250) NULL
)
WITH
(
	DISTRIBUTION = REPLICATE,
	HEAP
)
GO

