DROP PROC [DA_MDDE].[sp_StartEntity_Execution]
GO
CREATE PROC [DA_MDDE].[sp_StartEntity_Execution] @par_runid [UNIQUEIDENTIFIER],@par_schema [NVARCHAR](500),@par_mapping [NVARCHAR](500) ,@par_outcome [NVARCHAR](500) AS
/***************************************************************************************************
Script Name         sp_UpdateConfig_Execution.sql
Create Date:        2025-07-31
Author:             Jeroen Poll
Description:        PROC
Used By:            -
Parameter(s):       - 	  @par_runid						= Synapse Pipeline runid of this execution 
						, @par_schema [NVARCHAR] (500)		= Key or Name of column to update 
						, @par_mapping [NVARCHAR] (500)		= Value to update with 
						, @par_outcome						= Outcome of the load
												
Usage:              -
****************************************************************************************************
SUMMARY OF CHANGES
Date(yyyy-mm-dd)    Author              Comments
------------------- ------------------- ------------------------------------------------------------
2025-07-31	        Jeroen Poll         Initial Script V1.0 

***************************************************************************************************/
SET NOCOUNT ON;

BEGIN
	DECLARE @sql NVARCHAR(MAX) = ''
	SET @sql = CONCAT ('UPDATE [DA_MDDE].[ConfigExecution]  '
	, ' SET '
	, ' [LoadStartDateTime] = GETDATE() AT TIME ZONE ''UTC'' AT TIME ZONE ''W. Europe Standard Time'''
	, ' [LoadOutcome] =  ''Running'''
	, ' WHERE 1=1 '
	, ' AND [LoadRunId] = ', '''', @par_runid , ''''
	, ' AND [Schema] = ', '''', @par_schema , ''''
	, ' AND [Mapping] = ', '''', @par_mapping , ''''
	)
	EXEC sp_executesql @sql
END
GO


