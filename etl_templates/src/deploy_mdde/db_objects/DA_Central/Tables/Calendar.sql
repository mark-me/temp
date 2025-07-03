CREATE TABLE [DA_Central].[Calendar]
(
	[Calendar_Date] [date] NOT NULL,
	[IsHoliday] [int] NULL,
	[IsPublicHoliday] [int] NULL
)
WITH
(
	DISTRIBUTION = REPLICATE,
	HEAP
)
GO
