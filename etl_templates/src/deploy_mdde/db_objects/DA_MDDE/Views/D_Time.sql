CREATE VIEW [DA_MDDE].[D_Time]
AS
SELECT [TimeKey]
      ,[Time]
      ,[Hour]
      ,[MilitaryHour]
      ,[Minute]
      ,[Second]
      ,[AmPm]
      ,[StandardTime]
  FROM [DM_Dim].[Time]
  WHERE [X_IsCurrent] = 1
