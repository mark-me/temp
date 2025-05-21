CREATE FUNCTION [DA_MDDE].[fn_GetDefaultValueForDatatype]
(
  @data_type NVARCHAR(128)
)
RETURNS NVARCHAR(128)
AS
/****
Returns the default value for @data_type. (includes quotes around the returns value, when it's a string data type)

Accepts data_type 'NVARCHAR' or data_type_full 'NVARCHAR(255)' notations 
*/
BEGIN
  DECLARE @default_value   NVARCHAR(128) = N''
        , @data_type_short NVARCHAR(128) = N'' ;
  -- ensure to only retrieve the base type 
  SET @data_type_short = LEFT(@data_type, COALESCE(NULLIF(CHARINDEX('(', @data_type) - 1, -1), LEN(@data_type)))

  SET @default_value = CASE @data_type_short
                            WHEN 'bit' THEN '0'        /* bit 1, 0, or NULL */
                            WHEN 'tinyint' THEN '0'    /* tinyint	0 to 255 */
                            WHEN 'smallint' THEN '-1'  /* smallint	-32,768 to 32,767 */
                            WHEN 'int' THEN '-1'       /* int	-2,147,483,648 to 2,147,483,647 */
                            WHEN 'bigint' THEN '-1'    /* bigint	-9,223,372,036,854,775,808 to 9,223,372,036,854,775,807 */
                            WHEN 'numeric' THEN '-1'    
                            WHEN 'decimal' THEN '-1'
                            WHEN 'smallmoney' THEN '-1'
                            WHEN 'money' THEN '-1'
                            WHEN 'float' THEN '-1'
                            WHEN 'real' THEN '-1'
                            --
                            WHEN 'datetime' THEN '''19000101'''
                            WHEN 'smalldatetime' THEN '''19000101'''
                            WHEN 'date' THEN '''19000101'''
                            WHEN 'time' THEN '''00:00'''
                            WHEN 'datetimeoffset' THEN '''19000101'''
                            WHEN 'datetime2' THEN '''19000101'''
                            --
                            WHEN 'timestamp' THEN 'DEFAULT'
                            --
                            WHEN 'char' THEN '''-'''
                            WHEN 'varchar' THEN '''-'''
                            WHEN 'text' THEN '''-'''
                            WHEN 'nchar' THEN '''-'''
                            WHEN 'nvarchar' THEN '''-'''
                            WHEN 'ntext' THEN '''-'''
							WHEN 'varbinary' THEN '0x00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000'
                            ELSE ''''
                          END ;

  RETURN @default_value ;

END ;
GO

