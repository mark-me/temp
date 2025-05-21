CREATE TABLE [DA_MDDE].[CodeList]
(
	[CodeListKey] [bigint] IDENTITY(1,1) NOT NULL,
	[SourceSystem] [nvarchar](50) NULL,
	[ElementName] [nvarchar](50) NULL,
	[Code] [nvarchar](50) NULL,
	[LabelEN] [nvarchar](500) NULL,
	[DescriptionEN] [nvarchar](1000) NULL,
	[LabelNL] [nvarchar](500) NULL,
	[DescriptionNL] [nvarchar](1000) NULL,
	[X_StartDate] [date] NULL,
	[X_EndDate] [date] NULL,
	[X_HashKey] [varbinary](8000) NULL,
	[X_IsCurrent] [bit] NULL,
	[X_IsReplaced] [bit] NULL,
	[X_RunId] [nvarchar](50) NULL,
	[X_LoadDateTime] [datetime] NULL,
	[X_Bron] [nvarchar](50) NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	HEAP
)