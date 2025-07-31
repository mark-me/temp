CREATE TABLE [DA_MDDE].[ConfigExecution] (
	  [ConfigExecutionsKey] [bigint] IDENTITY(1, 1) NOT NULL 
	, [DateTime] [datetime2](7) NOT NULL
	, [LoadRunId] [nvarchar](100) NOT NULL

	, [Model] [nvarchar](255) NULL
	, [Schema] [nvarchar](255) NULL
	, [Mapping] [nvarchar](255) NULL
	, [Source] [nvarchar](255) NULL
	, [Destination] [nvarchar](255) NULL
	, [RunLevel] [int] NULL
	, [RunLevelStage] [int] NULL
	, [LoadType] [tinyint] NULL

	
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