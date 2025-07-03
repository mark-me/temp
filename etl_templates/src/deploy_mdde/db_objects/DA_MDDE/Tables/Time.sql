CREATE TABLE [DM_Dim].[Time] (
	  [TimeKey] [int] IDENTITY(1, 1) NOT NULL
	, [Time] [char](8) NOT NULL
	, [Hour] [char](2) NOT NULL
	, [MilitaryHour] [char](2) NOT NULL
	, [Minute] [char](2) NOT NULL
	, [Second] [char](2) NOT NULL
	, [AmPm] [char](2) NOT NULL
	, [StandardTime] [char](11) NULL
	, [X_StartDate] [date] NULL
	, [X_EndDate] [date] NULL
	, [X_HashKey] int NULL
	, [X_IsCurrent] [bit] NULL
	, [X_IsReplaced] [bit] NULL
	, [X_RunId] [int] NULL
	, [X_LoadDateTime] [datetime] NULL
	, [X_Bron] [nvarchar](250) NULL
	)
