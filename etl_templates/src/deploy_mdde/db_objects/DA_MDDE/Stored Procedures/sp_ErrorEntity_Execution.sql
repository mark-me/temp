CREATE PROC [DA_MDDE].[sp_ErrorEntity_Execution] @par_runid [UNIQUEIDENTIFIER],@par_schema [NVARCHAR](500),@par_mapping [NVARCHAR](500), @RowCountInsert [BigInt] , @RowCountUpdate [BigInt], @RowCountDelete [BigInt] AS
/***************************************************************************************************
Script Name         sp_ErrorEntity_Execution.sql
Create Date:        2025-08-05
Author:             Youri/Avinash
Description:        PROC
Used By:            -
Parameter(s):       - 	  @par_runid						= Synapse Pipeline runid of this execution 
						, @par_schema [NVARCHAR] (500)		= Key or Name of column to update 
						, @par_mapping [NVARCHAR] (500)		= Value to update with 
												
Usage:              -
****************************************************************************************************
SUMMARY OF CHANGES
Date(yyyy-mm-dd)    Author              Comments
------------------- ------------------- ------------------------------------------------------------
2025-08-05        Youri/Avinash         Initial Script V1.0 

***************************************************************************************************/
SET NOCOUNT ON;

BEGIN
	DECLARE @sql NVARCHAR(MAX) = ''
	SET @sql = CONCAT ('UPDATE [DA_MDDE].[ConfigExecution]  '
	, ' SET '
	, ' [LoadEndDateTime] = GETDATE() AT TIME ZONE ''UTC'' AT TIME ZONE ''W. Europe Standard Time'','
	, ' [RowCountInsert] = NULL'
	, ' [RowCountUpdate] = NULL'
	, ' [RowCountDelete] = NULL'
	, ' [LoadOutcome] =  ''NOK'''
	, ' WHERE 1=1 '
	, ' AND [LoadRunId] = ', '''', @par_runid , ''''
	, ' AND [Schema] = ', '''', @par_schema , ''''
	, ' AND [Mapping] = ', '''', @par_mapping , ''''
	)
	EXEC sp_executesql @sql
END
GO


