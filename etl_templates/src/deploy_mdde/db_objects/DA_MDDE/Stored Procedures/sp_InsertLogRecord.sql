CREATE PROC [DA_MDDE].[sp_InsertLogRecord] @LogID [UNIQUEIDENTIFIER],@ObjectID [BIGINT],@PipelineRunID [NVARCHAR](36),@ActivityID [NVARCHAR](36),@TriggerID [NVARCHAR](36),@SourceCode [NVARCHAR](10),@Object [NVARCHAR](200),@State [NVARCHAR](50),@User [nvarchar](128),@PipelineName [NVARCHAR](200),@TriggerName [NVARCHAR](200),@TriggerType [NVARCHAR](50),@StoredProcName [NVARCHAR](200),@StoredProcParameter [NVARCHAR](400),@LogMessage [NVARCHAR](400) AS

BEGIN
    -- SET NOCOUNT ON added to prevent extra result sets from
    -- interfering with SELECT statements.
    SET NOCOUNT ON

    IF @State NOT IN ('Started', 'Succeeded', 'Failed')
    BEGIN
        THROW 50000, 'Invalid state value. must be Started, Succeeded Or Failed', 1;
    END

    DECLARE @EventDateTime DATETIME2
    SET @LogID = COALESCE(@LogID, NEWID())
    SET @EventDateTime = CAST( GETDATE() AT TIME ZONE 'UTC' AT TIME ZONE 'Central european standard time' AS DATETIME2 )
	SET @User = COALESCE(@User, CURRENT_USER)

    -- Insert statements for procedure here
    INSERT INTO [DA_MDDE].[Logging]
	(
        LogID
		,ObjectID		
		,PipelineRunID	
        ,ActivityID
		,TriggerID		
		,Sourcecode
		,[Object]		
		,[State]
        ,[User]
		,PipelineName	
		,TriggerName	
		,TriggerType
		,StoredProcName	
		,StoredProcParameter
        ,LogMessage
		,EventDateTime		
	)

	VALUES
	(
        @LogID	
		,@ObjectID		
		,@PipelineRunID	
        ,@ActivityID
		,@TriggerID	
		,@Sourcecode
		,@Object		
		,@State
        ,@User
		,@PipelineName	
		,@TriggerName	
		,@TriggerType	
		,@StoredProcName	
		,@StoredProcParameter
        ,@LogMessage
		,@EventDateTime	
	)
	BEGIN
	 PRINT (CONCAT (CONVERT(NCHAR(24), GETDATE() AT TIME ZONE 'UTC' AT TIME ZONE 'W. Europe Standard Time', 121), ' || ' ,CAST( @PipelineRunID AS NCHAR(36))	, ' || ', CAST(@State AS NCHAR(12)) , ' || ' ,  @LogMessage))
	END
END
GO


