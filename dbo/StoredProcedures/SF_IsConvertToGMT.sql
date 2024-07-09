
CREATE PROCEDURE [dbo].[SF_IsConvertToGMT] 
	@table_server sysname,
	@IsConvertToGMT bit OUTPUT

AS

declare @otherValue nvarchar(1000) = ''
declare @sql nvarchar(1000) = ''

set @sql = 'SELECT @other = Value from openquery(' + @table_server + ', ' + '''' + 'Select Value from sys_connection_props where Name = ''''Other''''''' + ')'
Begin Try
	EXECUTE sp_executesql @sql, N'@other nvarchar(1000) OUTPUT', @other=@otherValue OUTPUT
	set @otherValue = LOWER(@otherValue)
	if CHARINDEX('convertdatetimetogmt=true',@otherValue) > 0
		set @IsConvertToGMT = 1
End Try
Begin Catch
	set @IsConvertToGMT = 0
End Catch

GO

