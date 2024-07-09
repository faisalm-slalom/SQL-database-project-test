
Create PROCEDURE [dbo].SF_GetTimestampField 
	@table_server sysname,
	@table_name sysname,
	@timestamp_field nvarchar(100) OUTPUT
AS

declare @systemModstampExists int = 0
declare @createdDateExists int = 0
declare @useCreatedDate bit = 0
declare @sql nvarchar(max) = ''
set @table_name = LOWER(@table_name)

if CHARINDEX('history', @table_name) > 0
	set @useCreatedDate = 1

set @sql = 'Select @cnt = Count(*) from openquery(' + @table_server + ', ' + '''' + 'select * from EntityParticle where EntityDefinitionId = ' + '''''' + @table_name + '''''' + ' and QualifiedApiName = ' + '''''' + 'SystemModstamp' + '''''''' + ')'
EXECUTE sp_executesql @sql, N'@cnt int OUTPUT', @cnt=@systemModstampExists OUTPUT

set @sql = 'Select @cnt = Count(*) from openquery(' + @table_server + ', ' + '''' + 'select * from EntityParticle where EntityDefinitionId = ' + '''''' + @table_name + '''''' + ' and QualifiedApiName = ' + '''''' + 'CreatedDate' + '''''''' + ')'
EXECUTE sp_executesql @sql, N'@cnt int OUTPUT', @cnt=@createdDateExists OUTPUT

if @systemModstampExists > 0
	set @timestamp_field = 'SystemModstamp'
else if @createdDateExists > 0 and @useCreatedDate = 1
	set @timestamp_field = 'CreatedDate'
else 
	set @timestamp_field = ''

GO

