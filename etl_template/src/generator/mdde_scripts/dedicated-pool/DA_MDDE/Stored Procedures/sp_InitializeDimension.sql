CREATE PROCEDURE [DA_MDDE].[sp_InitializeDimension]

  @par_runid [NVARCHAR](500)
, @par_layerName [NVARCHAR](500)
, @par_dimensionName [NVARCHAR](500)
, @par_debug                  [BIT]
AS
/**********************************************************************
*** All fields are set to a default value 
*** If the @identity = 1 then the `SET IDENTITY_INSERT` command is used to be able to insert a record into the indentity field

	     Date        Changed by              Change       Description

***********************************************************************/
BEGIN
	DECLARE @LogMessage		NVARCHAR(MAX);
	DECLARE @sql_delete     NVARCHAR(MAX);
	DECLARE @sql_insert     NVARCHAR(MAX);
	DECLARE @sql_identity_insert_on  NVARCHAR(MAX) = '';
	DECLARE @sql_identity_insert_off NVARCHAR(MAX) = '';


	IF (@par_debug = 1)
		EXEC [DA_MDDE].[sp_Logger] 'INFO', N'Debug is set to TRUE.'

	/* Delete existing dummy record */
	SET @sql_delete = N'DELETE FROM ' + QUOTENAME(@par_layerName) + '.' + QUOTENAME(@par_dimensionName) + N' WHERE ' + QUOTENAME(@par_dimensionName + 'Key') + N' = -1' ;
	
	EXEC [DA_MDDE].[sp_Logger] 'INFO', @sql_delete
	IF (@par_debug <> 0)
		EXEC sp_executesql @sql_delete;


	/* Check if table has identity column  */
	declare @identitycolumns int
	select @identitycolumns = count(*)  from sys.columns
	WHERE 1 = 1 
	AND OBJECT_SCHEMA_NAME(object_id) = @par_layerName
	AND OBJECT_NAME(object_id) = @par_dimensionName
	AND is_identity = 1

	/* set IDENTITY_INSERT to ON for table  */
	IF (@identitycolumns > 0)
		SET @sql_identity_insert_on = 'SET IDENTITY_INSERT '+  QUOTENAME(@par_layerName) + '.' + QUOTENAME(@par_dimensionName)+ ' ON;'

	/* Create insert statement for dummy record  */
	SET @sql_insert = 
		(select CONCAT(
						'INSERT INTO [', @par_layerName,'].[', @par_dimensionName,'] (', CHAR(13),  CHAR(10)
						, STRING_AGG( CHAR(09) + CONVERT(NVARCHAR(MAX), c.COLUMN_NAME),', '+ CHAR(13) + CHAR(10)) WITHIN GROUP (ORDER BY C.ORDINAL_POSITION ASC), CHAR(13),  CHAR(10)
						,CHAR(09) , ')'
						,CHAR(10), 'VALUES (', CHAR(10)
						, STRING_AGG( CHAR(09) + ' /* ' + c.COLUMN_NAME + ' */ ' + 
						/* Dummy record must be current. */
						CASE WHEN c.COLUMN_NAME = 'X_IsCurrent' THEN '1'
							 WHEN c.COLUMN_NAME = 'X_EndDate' THEN '''2099-12-31'''
							ELSE CONVERT(NVARCHAR(MAX), [DA_MDDE].[fn_GetDefaultValueForDatatype](c.DATA_TYPE))
						END
						,', '+ CHAR(13) + CHAR(10)) WITHIN GROUP (ORDER BY C.ORDINAL_POSITION ASC), CHAR(13),  CHAR(10)
						,CHAR(09) , ')'
						) 
		FROM INFORMATION_SCHEMA.COLUMNS AS c
		LEFT JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS kcu ON kcu.TABLE_NAME = c.TABLE_NAME AND kcu.TABLE_SCHEMA = c.TABLE_SCHEMA AND kcu.TABLE_CATALOG = c.TABLE_CATALOG AND kcu.COLUMN_NAME = c.COLUMN_NAME
		LEFT JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS tc ON kcu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME AND tc.CONSTRAINT_SCHEMA = kcu.CONSTRAINT_SCHEMA AND tc.CONSTRAINT_CATALOG = kcu.CONSTRAINT_CATALOG
		WHERE 1 = 1 
		AND c.TABLE_SCHEMA = @par_layerName 
		AND c.TABLE_NAME = @par_dimensionName)

	/* set IDENTITY_INSERT to OFF for table  */
	IF (@identitycolumns > 0)
		SET @sql_identity_insert_off = 'SET IDENTITY_INSERT '+  QUOTENAME(@par_layerName) + '.' + QUOTENAME(@par_dimensionName)+ ' OFF;'

	SET @sql_insert = CONCAT(CHAR(10) ,@sql_identity_insert_on, CHAR(10), CHAR(10) ,@sql_insert,CHAR(10), CHAR(10), @sql_identity_insert_off)
	EXEC [DA_MDDE].[sp_Logger] 'INFO', @sql_insert
	IF (@par_debug <> 1)
		EXEC sp_executesql @sql_insert


END ;
GO