Create PROCEDURE [dbo].[SF_IsTemporalTable] 
@table_name NVARCHAR(MAX), 
@IsTemporalTable bit OUTPUT

AS

declare @sql nvarchar(500)
declare @rowCount int = 0
set @IsTemporalTable = 0
set @table_name = LOWER(@table_name)

set @sql = 'Select @C = Count(name) from sys.tables where LOWER(name) = ' + '''' + @table_name + '''' 
			+ ' and schema_id = SCHEMA_ID(' + '''' + 'dbo' + '''' + ')' 
			+ ' and temporal_type_desc = ' + '''' + 'SYSTEM_VERSIONED_TEMPORAL_TABLE' + ''''
EXEC sp_executesql @sql, N'@C INT OUTPUT', @C = @rowCount OUTPUT

if @rowCount > 0
	set @IsTemporalTable = 1

GO

