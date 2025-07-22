import sqlfluff
content = '''
CREATE TABLE [DA_Central].[ActivityCode]
(
 [ActivityCodeKey]  bigint IDENTITY(1,1) NOT NULL
,[ActivityCodeBKey] nvarchar(200) NOT NULL 
,[ActivityCodeStartDate]  date
 ,[ActivityCodeEndDate]  date
 ,[ActivityCodeRegistrationDate]  date
 ,[ActivityCodeTimestamp]  datetime
 ,[ActivityCodeDescriptionShort]  nvarchar(20)
 ,[ActivityCodeDescriptionLong]  nvarchar(70)
 ,[ActivityCodeDescriptionLegal]  nvarchar(1024)
 ,[X_Startdate]    date
,[X_EndDate]  date
,[X_HashKey]  int  
,[X_IsCurrent]    bit
,[X_IsReplaced]   bit
,[X_RunId]    int
,[X_LoadDateTime] datetime
,[X_Bron] nvarchar(10)
)
WITH
(
    DISTRIBUTION = ROUND_ROBIN,
    HEAP
);
  '''

contentFixList = sqlfluff.lint(content, dialect="tsql")
content = sqlfluff.fix(content, dialect="tsql", exclude_rules = ['ST06','RF06'])
content2 = sqlfluff.fix(content, dialect="tsql", rules = ['LT01','LT02','LT04','LT05','LT13','CP05'])
print(content)
print(content2)