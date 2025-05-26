CREATE PROC [DA_MDDE].[sp_InsertConfigExecution]
@par_executionid UNIQUEIDENTIFIER,
@par_runid [NVARCHAR] (500),
@par_layername [NVARCHAR](500),
@par_mappingname [NVARCHAR](500),
@par_targetname [NVARCHAR](500)
AS
/***************************************************************************************************
Script Name         sp_InsertConfigExecution.sql
Create Date:        2025-05-20
Author:             Jeroen Poll
Description:        PROC
Used By:            -
Parameter(s):       - 	  @par_executionid						= ID of this execution 
						, @par_runid [NVARCHAR] (500)			= ETL RunID form the Synapse Pipeline. 
						, @par_LayerName [NVARCHAR] (500)		= Schema Name. 
						, @par_MappingName [NVARCHAR] (500)		= Mapping name from PowerDesigner
						, @par_targetname [NVARCHAR] (500) = Desitination table or view Name. 
												
Usage:              -
****************************************************************************************************
SUMMARY OF CHANGES
Date(yyyy-mm-dd)    Author              Comments
------------------- ------------------- ------------------------------------------------------------
2025-05-20	        Jeroen Poll         Initial Script V1.0 

***************************************************************************************************/
BEGIN
	INSERT INTO [DA_MDDE].[ConfigExecutions]
           ([ExecutionId]
		   ,[LoadRunId]
           ,[ConfigKey]
           ,[ModelName]
           ,[LayerName]
           ,[MappingName]
           ,[TargetName]
           ,[SourceName]
           ,[RunLevel]
           ,[RunLevelStage]
           ,[LoadType]
		   ,[StartLoadDateTime])
    SELECT      @par_executionid AS [ExecutionId]
            ,@par_runid AS [LoadRunId]
            ,[ConfigKey]
            ,[ModelName]
            ,[LayerName]
            ,[MappingName]
            ,[TargetName]
            ,[SourceName]
            ,[RunLevel]
            ,[RunLevelStage]
            ,[LoadType]
            ,GETDATE() AT TIME ZONE 'UTC' AT TIME ZONE 'W. Europe Standard Time' as [StartLoadDateTime]
    FROM [DA_MDDE].[Config] c
    WHERE 1=1 
    AND c.[LayerName] = @par_layername
    AND c.[MappingName] = @par_mappingname
    AND c.[TargetName] = @par_targetname
END
GO