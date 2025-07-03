CREATE PROC DA_MDDE.sp_TruncateAllTables
AS
--Get the list of all the tables to be truncated
IF OBJECT_ID('tempdb..#TablesToBeTruncated') IS NOT NULL
BEGIN
	DROP TABLE #TablesToBeTruncated
END

CREATE TABLE #TablesToBeTruncated (
	Id INT IDENTITY(1, 1)
	, TableObjectId INT
	, TableName SYSNAME
	, SchemaId INT
	)

INSERT INTO #TablesToBeTruncated (
	TableObjectId
	, TableName
	, SchemaId
	)
SELECT ST.object_id
	, ST.name
	, ST.schema_id
FROM sys.Tables ST
INNER JOIN sys.Schemas SS ON ST.schema_id = SS.schema_id
WHERE ST.type = 'U' AND ST.NAME NOT LIKE '#%' AND ST.name <> 'sysdiagrams' AND SS.name IN ('DA_Central', 'DM_Dim', 'DM_Fact') -- Specify here the comma separated schema names which tables need to be truncated
	AND ST.NAME NOT IN ('Calendar', 'Date') -- Specify here the comma separated table names which needs to be truncated
	--AND ST.NAME IN ('') -- Specify here the comma separated table names for which truncation is not required

PRINT 'BEGIN TRANSACTION TruncateTables'
PRINT 'BEGIN TRY'
PRINT '    --------TRUNCATE TABLES SCRIPT--------'

--TRUNCATE TABLES
DECLARE @id INT
	, @truncatescript NVARCHAR(MAX)

SELECT @id = MIN(Id)
FROM #TablesToBeTruncated

WHILE @id IS NOT NULL
BEGIN
	SELECT @truncatescript = '    TRUNCATE TABLE ' + QUOTENAME(SCHEMA_NAME(SchemaId)) + '.' + QUOTENAME(TableName)
	FROM #TablesToBeTruncated
	WHERE Id = @id

	PRINT CAST(@truncatescript AS NVARCHAR(MAX))

	SELECT @id = MIN(Id)
	FROM #TablesToBeTruncated
	WHERE Id > @id
END

PRINT '    COMMIT TRANSACTION TruncateTables'
PRINT 'END TRY'
PRINT 'BEGIN CATCH'
PRINT '    ROLLBACK TRANSACTION TruncateTables'
PRINT 'END CATCH'
GO


