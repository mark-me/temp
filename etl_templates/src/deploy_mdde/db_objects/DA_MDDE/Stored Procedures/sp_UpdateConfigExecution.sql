CREATE PROC [DA_MDDE].[sp_UpdateConfigExecution]
@par_executionid UNIQUEIDENTIFIER,
@par_key [NVARCHAR] (500),
@par_value [NVARCHAR](500)
AS
/***************************************************************************************************
Script Name         sp_UpdateConfigExecution.sql
Create Date:        2025-05-20
Author:             Jeroen Poll
Description:        PROC
Used By:            -
Parameter(s):       - 	  @par_executionid					= ID of this execution 
						, @par_key [NVARCHAR] (500)			= Key or Name of column to update 
						, @par_value [NVARCHAR] (500)		= Value to update with 
						 
												
Usage:              -
****************************************************************************************************
SUMMARY OF CHANGES
Date(yyyy-mm-dd)    Author              Comments
------------------- ------------------- ------------------------------------------------------------
2025-05-20	        Jeroen Poll         Initial Script V1.0 

***************************************************************************************************/
BEGIN
	DECLARE @sql NVARCHAR(MAX) = ''
	SET @sql = CONCAT ('UPDATE [DA_MDDE].[ConfigExecutions]  SET [', TRIM(@par_key), '] = ', '''', TRIM(@par_value), '''', ' WHERE [ExecutionId] = ', '''', @par_executionid , '''')
	print(@sql)
	EXEC sp_executesql @sql
END
GO