CREATE PROCEDURE [DA_MDDE].[sp_LoadDates]
AS
/***************************************************************************************************
Script Name         sp_LoadDates.sql
Create Date:        2024-10-10
Author:             Jeroen Poll
Description:        PROC
Used By:            -
Parameter(s):       -
Usage:              -
****************************************************************************************************
SUMMARY OF CHANGES
Date(yyyy-mm-dd)    Author              Comments
------------------- ------------------- ------------------------------------------------------------
2024-10-10	        Jeroen Poll         Initial Script
***************************************************************************************************/
BEGIN
IF (SELECT COUNT(*) FROM [DA_MDDE].[Dates] WHERE [Date] = CAST(GETDATE() AS DATE)) > 0 	
	print('Calendar tabel has data (for today)')
ELSE
	BEGIN TRY
		TRUNCATE TABLE [DA_MDDE].[Dates];

		INSERT INTO [DA_MDDE].[Dates] ([Date])
		SELECT a.[Calendar_Date]
		FROM (
			SELECT TOP 300000 /* needs a top x to prevent a overflow of the date datatype */
				DATEADD(day, (
						ROW_NUMBER() OVER (
							ORDER BY datum.object_id
								, datumrange.object_id ASC
							)
						), CAST('1899-12-31' AS DATE)) AS Calendar_Date
			FROM sys.objects AS datum
			CROSS JOIN sys.objects AS datumrange
			) a
		WHERE a.Calendar_Date BETWEEN '1900-01-01' AND '2100-12-31'
		GROUP BY a.[Calendar_Date]
	END TRY

	BEGIN CATCH
		PRINT ('')
		PRINT ('ERROR:')
		PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS VARCHAR(10));
		PRINT 'Error Message: ' + ERROR_MESSAGE();

		DECLARE @LogEndDate DATE;
		DECLARE @LogEndTime TIME;
		DECLARE @LogErrorNumber NVARCHAR(50);
		DECLARE @LogErrorProcedure NVARCHAR(100);
		DECLARE @LogErrorMessage NVARCHAR(1000);
		DECLARE @LogErrorState INT;
		DECLARE @LogErrorSeverity INT;

		SET @LogEndDate = GETDATE() AT TIME ZONE 'UTC' AT TIME ZONE 'W. Europe Standard Time'
		SET @LogEndTime = CONVERT(TIME, GETDATE() AT TIME ZONE 'UTC' AT TIME ZONE 'W. Europe Standard Time', 14)
		SET @LogErrorNumber = ERROR_NUMBER()
		SET @LogErrorProcedure = ERROR_PROCEDURE()
		SET @LogErrorMessage = ERROR_MESSAGE()
		SET @LogErrorState = ERROR_STATE()
		SET @LogErrorSeverity = ERROR_SEVERITY()

		-- Use RAISERROR inside the CATCH block to return error
		-- information about the original error that caused
		-- execution to jump to the CATCH block.
		RAISERROR (
				@LogErrorMessage
				, -- Message text.
				@LogErrorSeverity
				, -- Severity.
				@LogErrorState -- State.
				);
	END CATCH
END
GO