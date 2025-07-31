CREATE TABLE [DA_MDDE].[Config] (
	[ConfigKey] [bigint] IDENTITY(1, 1) NOT NULL
	, [ModelName] [nvarchar](255) NULL
	, [LayerName] [nvarchar](255) NULL
	, [MappingName] [nvarchar](255) NULL
	, [TargetName] [nvarchar](255) NULL
	, [SourceName] [nvarchar](255) NULL
	, [RunLevel] [int] NULL
	, [RunLevelStage] [int] NULL
	, [LoadTypeDefault] TINYINT NULL
	, [LoadType] TINYINT NULL
	, [LoadCommand] [nvarchar](1000) NULL
	, [LoadRunId] [nvarchar](100) NULL
	, [LoadStartDateTime] [datetime2](7) NULL
	, [LoadEndDateTime] [datetime2](7) NULL
	, [RowCountInsert] [bigint] NULL
	, [RowCountUpdate] [bigint] NULL
	, [RowCountDelete] [bigint] NULL
	, [LoadOutcome] [nvarchar](100) NULL
	)
	WITH (
			DISTRIBUTION = ROUND_ROBIN
			, HEAP
			)
