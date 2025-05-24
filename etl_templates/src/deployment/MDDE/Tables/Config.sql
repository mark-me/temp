CREATE TABLE [DA_MDDE].[Config] (
	 [ConfigKey] [bigint] IDENTITY(1, 1) NOT NULL
	,[ModelName] [nvarchar](255) NULL   
	,[LayerName] [nvarchar](255) NULL   
	,[MappingName] [nvarchar](255) NULL
	,[TargetName] [nvarchar](255) NULL
	,[SourceName] [nvarchar](255) NULL
	,[RunLevel] [int] NULL
	,[RunLevelStage] [int] NULL
	,[MaxTimestamp] DATETIME2 NULL
	,[MaxTimestamp_LastRun] DATETIME2 NULL
	,[LoadType] BIT NOT NULL
	,[LoadCommand] [nvarchar](1000) NULL
	,[LoadRunId] [nvarchar](100) NULL
	,[LoadDateTime] [datetime2](7) NULL
	,[LoadOutcome] [nvarchar](100) NULL
	)
	WITH (
			DISTRIBUTION = ROUND_ROBIN
			,HEAP
			)