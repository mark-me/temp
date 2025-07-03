CREATE VIEW [DA_MDDE].[D_Mapping]
AS
SELECT 
MappingKey = ROW_NUMBER() OVER ( ORDER BY [MappingName])
,[MappingName]
FROM(
SELECT  [MappingName]
FROM [DA_MDDE].[ConfigExecutions]
GROUP BY [MappingName]) a
