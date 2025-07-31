CREATE TABLE [DA_MDDE].[ConfigBase]
(
	[ConfigBaseKey] [bigint] IDENTITY(1,1) NOT NULL,
	[Model] [nvarchar](255) NULL,
	[Schema] [nvarchar](255) NULL,
	[Mapping] [nvarchar](255) NULL,
	[Source] [nvarchar](255) NULL,
	[Destination] [nvarchar](255) NULL,
	[RunLevel] [int] NULL,
	[RunLevelStage] [int] NULL,
	[LoadType] [tinyint] NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	HEAP
)
GO
