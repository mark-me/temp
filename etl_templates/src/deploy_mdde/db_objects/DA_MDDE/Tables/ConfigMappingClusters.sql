CREATE TABLE [DA_MDDE].[ConfigMappingClusters] (
	[Schema] [nvarchar](255) NULL
	, [Mapping] [nvarchar](255) NULL
	, [Cluster] [int] NULL
	)
	WITH (
			DISTRIBUTION = ROUND_ROBIN
			, HEAP
			)