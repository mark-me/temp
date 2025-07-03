CREATE TABLE [DA_MDDE].[ConfigExecutions]
(
	[ConfigExecutionsKey] [bigint] IDENTITY(1,1) NOT NULL,
	[ExecutionId] UNIQUEIDENTIFIER  NOT NULL,
	[LoadRunId] [nvarchar](100) NULL,
	[ConfigKey] [bigint]  NULL,
	[ModelName] [nvarchar](255) NULL,
	[LayerName] [nvarchar](255) NULL,
	[MappingName] [nvarchar](255) NULL,
	[TargetName] [nvarchar](255) NULL,
	[SourceName] [nvarchar](255) NULL,
	[RunLevel] [int] NULL,
	[RunLevelStage] [int] NULL,
	[LoadType] [bit] NOT NULL,
	[StartLoadDateTime] [datetime2](7) NULL,
	[EndLoadDateTime] [datetime2](7) NULL,
	[RowCountInsert] [bigint]  NULL,
	[RowCountUpdate] [bigint]  NULL,
	[RowCountDelete] [bigint]  NULL,
	[LoadOutcome] [nvarchar](100) NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	HEAP
)