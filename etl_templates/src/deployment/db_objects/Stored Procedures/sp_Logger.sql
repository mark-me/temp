CREATE PROC [DA_MDDE].[sp_Logger] @MessageType [NVARCHAR](50),@Message [NVARCHAR](max) AS
BEGIN
	DECLARE @LogDate AS NVARCHAR(50);

	SET @LogDate = CONVERT(NVARCHAR(50), GETDATE(), 121);
	SET @Message = @LogDate + ' ' + @MessageType + ': ' + @Message

	IF @MessageType IN (
			'INFO'
			,'WARNING'
			)
		PRINT (@Message)
	ELSE IF @MessageType = 'ERROR'
		RAISERROR (
				@Message
				,10
				,1
				)
		WITH NOWAIT
END
GO