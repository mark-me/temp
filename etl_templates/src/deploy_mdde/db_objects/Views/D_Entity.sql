CREATE VIEW [DA_MDDE].[D_Entity]
AS
SELECT 
EntityKey = ROW_NUMBER() OVER ( ORDER BY EntityName)
,* 
FROM(
SELECT  EntityName = [TargetName]
FROM [DA_MDDE].[ConfigExecutions]
GROUP BY [TargetName]) a
