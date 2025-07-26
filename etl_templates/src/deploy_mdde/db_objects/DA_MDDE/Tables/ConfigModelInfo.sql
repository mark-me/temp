CREATE TABLE [DA_MDDE].[ConfigModelInfo] (
	[ConfigKey] [bigint] IDENTITY(1, 1) NOT NULL
	, [FilenamePowerDesigner] [nvarchar](255) NULL
	, [FilenameRepo] [nvarchar](255) NULL
	, [Creator] [nvarchar](255) NULL
	, [DateCreated] [nvarchar](255) NULL
	, [Modifier] [nvarchar](255) NULL
	, [DateModified] [nvarchar](255) NULL
	, [OrderProcessed] [nvarchar](255) NULL
	, [FileRETW] [nvarchar](255) NULL
	, [FileRETWCreationDate] [nvarchar](255) NULL
	, [FileRETWModificationDate] [nvarchar](255) NULL
	)
	WITH (
			DISTRIBUTION = ROUND_ROBIN
			, HEAP
			)
