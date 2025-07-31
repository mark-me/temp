CREATE PROC [DA_MDDE].[sp_LoadEntity_Incremental] @par_runid [NVARCHAR](500),@par_Schema [NVARCHAR](500),@par_Source [NVARCHAR](500),@par_Destination [NVARCHAR](500),@par_Mapping [NVARCHAR](500),@par_Debug [Bit] AS
/***************************************************************************************************
Script Name         sp_LoadEntityData_IncrementalLoad.sql
Create Date:        2025-02-17
Author:             Jeroen Poll
Description:        PROC
Used By:            -
Parameter(s):       - @par_runid [NVARCHAR] (500)  /* ETL RunID form the Synapse Pipeline. */
					, @par_Schema [NVARCHAR] (500)  /* Schema Name. */
					, @par_Source [NVARCHAR] (500) /* Source table or view Name. */
					, @par_Destination [NVARCHAR] (500) /* Desitination table or view Name. */
					, @par_Mapping [NVARCHAR] (500) /* Mapping name from PowerDesigner. Is only used for logging.  */
Usage:              -
****************************************************************************************************
SUMMARY OF CHANGES
Date(yyyy-mm-dd)    Author              Comments
------------------- ------------------- ------------------------------------------------------------
2025-07-21			
***************************************************************************************************/
SELECT 1 as a
GO