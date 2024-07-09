
CREATE PROCEDURE SF_Generate
	@operation nvarchar(20), 
	@table_server sysname,
	@load_tablename	sysname 

AS

-- Input Parameter @operation - Must be either 'Insert','Upsert','Update','Delete'
-- Input Parameter @table_server - Linked Server Name
-- Input Parameter @load_tablename - Existing bulkops table
print N'--- Starting SF_Generate' + ' ' +  dbo.SF_Version()
if LOWER(@operation) not in ('insert','upsert','update','delete')
begin
	RAISERROR ('--- Ending SF_Generate. Error: Invalid operation parameter.',16,1)
	return 1
end

IF  EXISTS (SELECT 1
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_TYPE='BASE TABLE' 
    AND TABLE_NAME=@load_tablename)
begin
	RAISERROR ('--- Ending SF_Generate. Error: Table to generate already exists.',16,1)
	return 1
end

set NOCOUNT ON

declare @base_tablename sysname
declare @work sysname
declare @sql nvarchar(max)
declare @uscore_pos int
declare @uuc_pos int
declare @uue_pos int
declare @uus_pos int
declare @uuh_pos int
declare @uuk_pos int
declare @uum_pos int
declare @rc int

set @operation = LOWER(@operation)

-- derive base table name from load table name
set @work = LOWER(@load_tablename)

-- Is this a custom table
set @uuc_pos = 0
set @uue_pos = 0
set @uus_pos = 0
set @uuh_pos = 0
set @uuk_pos = 0
set @uum_pos = 0

declare @highest int
set @highest = 0

IF (select CHARINDEX(reverse('__c'),reverse(@work)))>0
begin
   Set @uuc_pos=Len(@work)-CHARINDEX(reverse('__c'),reverse(@work))+1
   set @highest = @uuc_pos
end

 
IF (select CHARINDEX(reverse('__e'),reverse(@work)))>0
begin
   Set @uue_pos=Len(@work)-CHARINDEX(reverse('__e'),reverse(@work))+1
   if @uue_pos > @highest set @highest = @uue_pos
end
 
IF (select CHARINDEX(reverse('__share'),reverse(@work)))>0
begin
   Set @uus_pos= Len(@work)-CHARINDEX(reverse('__share'),reverse(@work))+1
   if @uus_pos > @highest set @highest = @uus_pos
end
 
IF (select CHARINDEX(reverse('__history'),reverse(@work)))>0
begin
   Set @uuh_pos= Len(@work)-CHARINDEX(reverse('__history'),reverse(@work))+1
   if @uuh_pos > @highest set @highest = @uuh_pos
end
 
IF (select CHARINDEX(reverse('__kav'),reverse(@work)))>0
begin
   Set @uuk_pos= Len(@work)-CHARINDEX(reverse('__kav'),reverse(@work))+1
	if @uuk_pos > @highest set @highest = @uuk_pos
end

IF (select CHARINDEX(reverse('__mdt'),reverse(@work)))>0
begin
   Set @uum_pos= Len(@work)-CHARINDEX(reverse('__mdt'),reverse(@work))+1
	if @uum_pos > @highest set @highest = @uum_pos
end

if @highest <> 0 
	begin
		set @base_tablename = SUBSTRING(@work,1,@highest)
	end
else
	begin
		set @uscore_pos = CHARINDEX('_',@work)

		if @uscore_pos = 0
			set @base_tablename = @work
		else
			set @base_tablename = SUBSTRING(@work,1,@uscore_pos-1)
	end

print 'Base table is ' + @base_tablename

-- Get a temporary sfcolumns table
CREATE TABLE #sfcolumns(
	[ColumnName] [nvarchar](2000),
	[DataType] [nvarchar](2000),
	[ColumnLength] [bigint],
	[Precision] [bigint],
	[Scale] [bigint]
)

set @sql = 'Insert into #sfcolumns '
set @sql = @sql + 'Select [COLUMN_NAME],[DATA_TYPE],[CHARACTER_MAXIMUM_LENGTH],[NUMERIC_PRECISION],[NUMERIC_SCALE] '
set @sql = @sql + 'from '+ @table_server + '.[CData].INFORMATION_SCHEMA.Columns where TABLE_NAME = ''' + @base_tablename + ''''
exec (@sql)
IF (@@ERROR <> 0) GOTO ERR_HANDLER

-- Get a temporary sfparticles table
CREATE TABLE #sfparticles(
	[QualifiedApiName] [nvarchar](255),
	[IsCreatable] [bit],
	[IsUpdatable] [bit]
)

set @sql = 'Insert into #sfparticles '
set @sql = @sql + 'Select [QualifiedApiName],[IsCreatable],[IsUpdatable] '
set @sql = @sql + 'from '+ @table_server + '.[CData].[Salesforce].EntityParticle where EntityDefinitionId = ''' + @base_tablename + ''''
exec (@sql)
IF (@@ERROR <> 0) GOTO ERR_HANDLER

-- Get a temporary sffields table
CREATE TABLE #sffields(
	[ColumnName] [nvarchar](2000),
	[DataType] [nvarchar](2000),
	[ColumnLength] [bigint],
	[Precision] [bigint],
	[Scale] [bigint],
	[IsCreatable] [bit],
	[IsUpdatable] [bit]
)

set @sql = 'Insert into #sffields '
set @sql = @sql + 'Select a.[ColumnName],a.[DataType],a.[ColumnLength],a.[Precision],a.[Scale],b.[IsCreatable],b.[IsUpdatable] '
set @sql = @sql + 'from #sfcolumns a, #sfparticles b where a.ColumnName = b.QualifiedApiName'
exec (@sql)
IF (@@ERROR <> 0) GOTO ERR_HANDLER

-- If no rows left then base table is not salesforce object
declare @rowcount int
Set @rowcount = (select count(*) from #sffields)

if (@rowcount = 0)
begin
	print 'Salesforce object ' + @base_tablename + ' does not exist'
	goto ERR_HANDLER
end

set @sql = 'Create Table ' + @load_tablename + ' ('
set @sql = @sql + '[Id] nvarchar(18) null'
set @sql = @sql + ',[Error] nvarchar(2000) null'

-- Generate rest of columns
declare @SQLDefinition nvarchar(512)
declare @Name nvarchar(2000)
declare @DataType nvarchar(2000)
declare @ColumnLength bigint
declare @Precision bigint
declare @Scale bigint
declare @IsCreatable bit
declare @IsUpdatable bit

declare flds_cursor cursor local fast_forward
for select [ColumnName],[DataType],[ColumnLength],[Precision],[Scale],[IsCreatable],[IsUpdatable] from #sffields

open flds_cursor
IF (@@ERROR <> 0) GOTO ERR_HANDLER

while 1 = 1
begin
	fetch next from flds_cursor into @Name,@DataType,@ColumnLength,@Precision,@Scale,@IsCreatable,@IsUpdatable
	if @@error <> 0 or @@fetch_status <> 0 break
	

	if LOWER(@Name) = 'id' continue
	
	set @DataType = LOWER(@DataType)
	if @DataType = 'nvarchar'
	Begin
		if @ColumnLength = -1
			set @SQLDefinition = 'nvarchar(max)'
		Else
			set @SQLDefinition = 'nvarchar(' + CAST(@ColumnLength AS nvarchar) + ')'
	End
	else if @DataType = 'decimal'
		set @SQLDefinition = 'numeric(' + CAST(@Precision AS nvarchar) + ',' + CAST(@Scale AS nvarchar) + ')'
	else if @DataType = 'float' or @DataType = 'int' or @DataType = 'bit' or @DataType = 'time' or @DataType = 'datetime2' or @DataType = 'date' or @DataType = 'varbinary'
		set @SQLDefinition = @DataType

	if @operation in ('insert','upsert') and @IsCreatable = 1
		set @sql = @sql + ',[' + @Name + '] ' + @SQLDefinition
	else if @operation in ('update') and @IsUpdatable = 1
		set @sql = @sql + ',[' + @Name + '] ' + @SQLDefinition
end

set @sql = @sql + ')'

close flds_cursor
deallocate flds_cursor

-- Print CREATE TABLE and execute it to create the table
print @sql
exec (@sql)
set @rc = 0

-- Return to caller
if @rc = 1
	RAISERROR ('--- Ending SF_Generate. Operation FAILED.',16,1)

SET NOCOUNT OFF
return @rc

ERR_HANDLER:
RAISERROR ('--- Ending SF_Generate. Operation FAILED.',16,1)
SET NOCOUNT OFF
return 1

GO

