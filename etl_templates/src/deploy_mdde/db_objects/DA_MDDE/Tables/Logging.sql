CREATE TABLE [DA_MDDE].[Logging]
(
	[RecordID] [bigint] IDENTITY(1,1) NOT NULL,
	[LogID] [uniqueidentifier] NULL,
	[ObjectID] [bigint] NULL,
	[PipelineRunID] [nvarchar](36) NULL,
	[ActivityID] [nvarchar](36) NULL,
	[TriggerID] [nvarchar](36) NULL,
	[SourceCode] [nvarchar](200) NULL,
	[Object] [nvarchar](200) NULL,
	[State] [nvarchar](50) NULL,
	[User] [nvarchar](128) NULL,
	[PipelineName] [nvarchar](200) NULL,
	[TriggerName] [nvarchar](200) NULL,
	[TriggerType] [nvarchar](50) NULL,
	[StoredProcName] [nvarchar](200) NULL,
	[StoredProcParameter] [nvarchar](4000) NULL,
	[LogMessage] [nvarchar](4000) NULL,
	[EventDateTime] [datetime2](7) NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	CLUSTERED COLUMNSTORE INDEX
)
GO


