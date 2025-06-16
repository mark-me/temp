CREATE PROC [DA_MDDE].[sp_Logger] @MessageType [NVARCHAR](50),@Message [NVARCHAR](max) AS
BEGIN
	DECLARE @printMessage NVARCHAR(MAX)
	SET @printMessage = CASE WHEN CHARINDEX('¡',@Message) = 0 THEN @Message ELSE substring(@Message, CHARINDEX('¡',@Message) + 1, len(@Message)) END
	IF @MessageType IN (
			'INFO'
			,'WARNING'
			)
		PRINT (@printMessage)
	ELSE IF @MessageType = 'ERROR'
		RAISERROR (
				@printMessage
				,10
				,1
				)
		WITH NOWAIT

	SET NOCOUNT ON

	-- Insert statements for procedure here
	INSERT INTO [DA_MDDE].[Logger] (
		[LogDate],
		[RunID],
		[MessageType],
		[Message]
		)
	SELECT 
		GETDATE() AT TIME ZONE 'UTC' AT TIME ZONE 'W. Europe Standard Time',
	    CASE WHEN CHARINDEX('¡',@Message) = 0 THEN '' ELSE substring(@Message, 0, CHARINDEX('¡',@Message)-1) END,
		@MessageType,
	    @printMessage
END
GO
