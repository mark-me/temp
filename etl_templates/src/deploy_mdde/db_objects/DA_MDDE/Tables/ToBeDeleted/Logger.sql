CREATE TABLE [DA_MDDE].[Logger]
(
	[LogDate] [datetime] NULL,
	[RunID] [nvarchar](50) NULL,
	[MessageType] [nvarchar](10) NULL,
	[Message] [nvarchar](max) NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	HEAP
)