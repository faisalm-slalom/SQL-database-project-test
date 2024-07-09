
CREATE PROCEDURE [dbo].[SF_Refresh]
	@table_server sysname,
	@table_name sysname,
	@schema_error_action varchar(100) = 'no',
	@error_messages_only nvarchar(20) = 'no',
	@verify_action varchar(100) = 'no',
	@bulkapi_option varchar(100) = null,
	@timestamp_field nvarchar(100) = null
AS
-- NOTE: This stored procedure will not work on SQL 2000.
--
-- Parameters: @table_server           - Salesforce Linked Server name (i.e. SALESFORCE)
--             @table_name             - Salesforce object to copy (i.e. Account)
--             @schema_error_action    - Controls the action for a schema change 
--                                     -    'No' : FAIL on a schema change
--                                     -    'Yes' : The table will be replicated instead
--									   -	'NoDrop': The table will be replicated with NoDrop
--                                     -    'Subset' : The new columns are ignored and the current
--                                                     subset of local table columns are refreshed.
--                                     -               Columns deleted on salesforce ARE NOT deleted locally. 
--                                     -    'SubsetDelete' : The new columns are ignored and the current
--                                                     subset of local table columns are refreshed.
--                                     -               Columns deleted on salesforce ARE deleted locally. 
--									   -    'Repair' :  The Max(SystemModStamp of the local table is used and 
--                                                      alternate method of handling deletes is used (slower)
--             @verify_action		   - Controls the row count compare behavior
--                                     -    'No' : Do not compare row counts
--                                     -    'Warn' : Compare row counts and issue warning if different
--                                     -    'Fail' : Compare row counts and fail the proc if different
--             @bulkapi_option  		- Options for using the bulkapi for the delta table
--    
declare @ProgDir   	varchar(250) 
set @ProgDir = 'C:\"Program Files"\CData\"CData DBAmp"\bin\'

declare @Command 	nvarchar(4000)
declare @Result 	int
declare @sql		nvarchar(max)
declare @parmlist	nvarchar(4000)
declare @columnList nvarchar(max)
declare @deletecolumnList nvarchar(max)
declare @colname	nvarchar(500)
declare @time_now	char(8)
set NOCOUNT ON

print '--- Starting SF_Refresh for ' + @table_name + ' ' +  dbo.SF_Version()
declare @LogMessage nvarchar(max)
declare @SPName nvarchar(50)
set @SPName = 'SF_Refresh:' + Convert(nvarchar(255), NEWID(), 20)
set @LogMessage = 'Parameters: ' + @table_server + ' ' + @table_name + ' '+ @schema_error_action + ' ' 
set @LogMessage = @LogMessage + ' ' + @verify_action + ' '+ ISNULL(@bulkapi_option, ' ') + ' Version: ' +  dbo.SF_Version()
exec SF_Logger @SPName, N'Starting', @LogMessage

declare @delim_table_name sysname
declare @refresh_table sysname
declare @delim_refresh_table sysname
declare @delta_table sysname
declare @delim_delta_table sysname
declare @deleted_table sysname
declare @deleted_table_ts sysname
declare @delim_deleted_table sysname

declare @server sysname
declare @database sysname
declare @timestamp_col_name nvarchar(2000) = ''
declare @is_history_table int
declare @diff_schema_count int
declare @options nvarchar(255) = ''
declare @usingOptions bit = 0

set @error_messages_only = Lower(@error_messages_only)
Select @time_now = (select Convert(char(8),CURRENT_TIMESTAMP, 8))

-- Validate parameters
if  (@error_messages_only <> 'yes' and @error_messages_only <> 'no')
begin
	print @time_now + ': Error: Invalid Error Messages Only Parameter: ' + @error_messages_only
	set @LogMessage = 'Error: Invalid Error Messages Only Parameter: ' + @error_messages_only
	exec SF_Logger @SPName, N'Message', @LogMessage
  	GOTO ERR_HANDLER
end

declare @isTemporalTable bit
set @isTemporalTable = 0
exec SF_IsTemporalTable @table_name, @isTemporalTable Output

if @isTemporalTable = 1
Begin
	Select @time_now = (select Convert(char(8),CURRENT_TIMESTAMP, 8))
	print @time_now + ': Error: ' + @table_name + ' must not be a Temporal table in order to refresh.' 
    set @LogMessage = 'Error: ' + @table_name + ' must not be a Temporal table in order to refresh.'
    exec SF_Logger @SPName, N'Message', @LogMessage
    GOTO ERR_HANDLER
End

declare @big_object_index int
set @big_object_index = CHARINDEX(REVERSE('__b'),REVERSE(@table_name))

if (@big_object_index = 1)
Begin
	Select @time_now = (select Convert(char(8),CURRENT_TIMESTAMP, 8))
	print @time_now + ': Error: Big Objects are not supported with SF_Refresh ' 
    set @LogMessage = 'Error: Big Objects are not supported with SF_Refresh'
    exec SF_Logger @SPName, N'Message', @LogMessage
    GOTO ERR_HANDLER
End

set @schema_error_action = Lower(@schema_error_action)
set @verify_action = Lower(@verify_action)
Select @time_now = (select Convert(char(8),CURRENT_TIMESTAMP, 8))

-- Validate parameters
if  @schema_error_action is null
   begin
	  print @time_now + ': Error: Invalid Schema Action Parameter: Cannot be Null ' 
	  set @LogMessage = 'Error: Invalid Schema Action Parameter: Cannot be Null'
	  exec SF_Logger @SPName, N'Message', @LogMessage
  	  GOTO ERR_HANDLER
   end
if  @schema_error_action <> 'yes' and
    @schema_error_action <> 'no' and
	@schema_error_action <> 'nodrop' and
     @schema_error_action <> 'subset' and
     @schema_error_action <> 'repair' and
      @schema_error_action <> 'subsetdelete' 
   begin
	  print @time_now + ': Error: Invalid Schema Action Parameter: ' + @schema_error_action
	  set @LogMessage = 'Error: Invalid Schema Action Parameter: ' + @schema_error_action
	  exec SF_Logger @SPName, N'Message', @LogMessage
  	  GOTO ERR_HANDLER
   end
   
if @schema_error_action <> 'no'
begin
	if @error_messages_only = 'no'
	Begin
		print @time_now + ': Using Schema Error Action of ' + @schema_error_action
		set @LogMessage = 'Using Schema Error Action of ' + @schema_error_action
		exec SF_Logger @SPName, N'Message', @LogMessage
	End
end

if  @verify_action <> 'no' and
    @verify_action <> 'warn' and
     @verify_action <> 'fail' 
   begin
	  print @time_now + ': Error: Invalid Verify Action Parameter: ' + @verify_action
	  set @LogMessage = 'Error: Invalid Verify Action Parameter: ' + @verify_action
	  exec SF_Logger @SPName, N'Message', @LogMessage
  	  GOTO ERR_HANDLER
   end

-- Put delimeters around names so we can name tables User, etc...
set @delim_table_name = '[' + @table_name + ']'
set @refresh_table = 'TableRefreshTime'
set @delim_refresh_table = '[' + @refresh_table + ']'
set @delta_table = @table_name + '_Delta' + CONVERT(nvarchar(30), GETDATE(), 126) 
set @delta_table = REPLACE(@delta_table, '-', '')
set @delta_table = REPLACE(@delta_table, ':', '')
set @delta_table = REPLACE(@delta_table, '.', '')
set @delim_delta_table = '[' + @delta_table + ']'
set @deleted_table = @table_name + '_Deleted'
set @deleted_table_ts = @deleted_table + CONVERT(nvarchar(30), GETDATE(), 126)
set @delim_deleted_table = '[' + @deleted_table_ts + ']'


-- Determine whether the local table and the previous copy exist
declare @table_exist int
declare @refresh_exist int
declare @delta_exist int
declare @deleted_exist int
declare @char_count varchar(10)

set @table_exist = 0
IF EXISTS (SELECT 1
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_TYPE='BASE TABLE' 
    AND TABLE_NAME=@table_name)
        set @table_exist = 1
IF (@@ERROR <> 0) GOTO ERR_HANDLER

-- Create table to track refresh times
set @refresh_exist = 0
IF EXISTS (SELECT 1
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_TYPE='BASE TABLE' 
    AND TABLE_NAME=@refresh_table)
        set @refresh_exist = 1
IF (@@ERROR <> 0) GOTO ERR_HANDLER
if (@refresh_exist = 0)
begin
   exec ('Create Table ' + @refresh_table + ' (TblName nvarchar(255) null, LastRefreshTime datetime null default CURRENT_TIMESTAMP) ')
   IF (@@ERROR <> 0) GOTO ERR_HANDLER
end

--Parse out actual table name if ColumnSubset is being used in order to use DBAmpTableOptions
declare @actualTableName sysname
declare @indexColSubset int
set @indexColSubset = (select CHARINDEX('_columnsubset', LOWER(@table_name)))
if @indexColSubset > 0
Begin
	set @actualTableName = (select LEFT(@table_name, @indexColSubset - 1))
End
else
Begin
	set @actualTableName = @table_name
End

--If table name in DBAmpTableOptions get options. If specified more than one different way, get one for actual table name.
if exists(Select TableName from DBAmpTableOptions where @actualTableName like TableName and Options is not null) 
Begin
	set @usingOptions = 1
	set @options = (Select top 1 Options from DBAmpTableOptions where @actualTableName like TableName order by TableName desc)
End

if @options is null and @bulkapi_option is not null
	set @options = @bulkapi_option

-- If table does not exist then replicate it
if (@table_exist = 0)
begin
	if @error_messages_only = 'no'
	Begin
		Select @time_now = (select Convert(char(8),CURRENT_TIMESTAMP, 8))
		print @time_now + ': Local table does not exist. Using SF_Replicate to create the local table.'
		set @LogMessage = 'Local table does not exist. Using SF_Replicate to create the local table.'
		exec SF_Logger @SPName, N'Message', @LogMessage
	End
	goto REPLICATE_EXIT
end

declare @replicateable bit

IF object_id('tempdb..#tmpSFEntityDefinition') IS NULL 
Begin
	-- Get the flags from DBAmp for this table
	set @sql = 'Select @RFOUT = Cast(IsReplicateable as bit) from openquery(' + @table_server + ', ' + '''' + 'Select IsReplicateable from EntityDefinition where QualifiedApiName = ' + '''''' + @actualTableName + '''''''' + ')'
	EXECUTE sp_executesql @sql, N'@RFOUT bit OUTPUT', @RFOUT=@replicateable OUTPUT
	IF (@@ERROR <> 0) GOTO ERR_HANDLER
End
else
Begin
	-- Get the flags from DBAmp for this table
	set @sql = 'Select @RFOUT = Cast(IsReplicateable as bit) from #tmpSFEntityDefinition where [Name] = ' + '''' + @actualTableName + ''''
	EXECUTE sp_executesql @sql, N'@RFOUT bit OUTPUT', @RFOUT=@replicateable OUTPUT
	IF (@@ERROR <> 0) GOTO ERR_HANDLER
End

if @timestamp_field IS NULL
Begin
	Begin try
		exec SF_GetTimestampField @table_server, @actualTableName, @timestamp_col_name out
	End Try
	Begin Catch
		Select @time_now = (select Convert(char(8),CURRENT_TIMESTAMP, 8))
		print @time_now + ': Error when getting TimestampField. ' + ERROR_MESSAGE()
		set @LogMessage = 'Error when getting TimestampField. ' + ERROR_MESSAGE()
		exec SF_Logger @SPName, N'Message', @LogMessage 
		GOTO ERR_HANDLER
	End Catch
End
else
Begin
	set @timestamp_col_name = @timestamp_field
End

if (@timestamp_col_name = 'CreatedDate') 
begin
	set @is_history_table = 1
end
else if (@timestamp_col_name = 'SystemModstamp')
begin
	set @is_history_table = 0
end
else if (@replicateable is null)
begin
-- query failed to return result
	Select @time_now = (select Convert(char(8),CURRENT_TIMESTAMP, 8))
	print @time_now + ': Error: Unable to retrieve EntityDefinition info for: ' + @actualTableName
	set @LogMessage = 'Error: Unable to retrieve EntityDefinition info for: ' + @actualTableName
	exec SF_Logger @SPName, N'Message', @LogMessage
	GOTO ERR_HANDLER
end
else
begin
	-- Cannot do a normal refresh because the table has no timestamp column
	if @error_messages_only = 'no'
	Begin
  		Select @time_now = (select Convert(char(8),CURRENT_TIMESTAMP, 8))
		print @time_now + ': Table does not contain a timestamp column needed to refresh. Using SF_Replicate to create table.'
		set @LogMessage = 'Table does not contain a timestamp column needed to refresh. Using SF_Replicate to create table.'
		exec SF_Logger @SPName, N'Message', @LogMessage
	End
	goto REPLICATE_EXIT
end

-- Get the last refresh time from the refresh table
-- This serves as the 'last run' time for the refresh
-- We subtract 30 mins to allow for long units of work on the salesforce side
declare @last_time smalldatetime
declare @last_time_GMT smalldatetime
declare @last_time_local smalldatetime
declare @table_crtime smalldatetime

-- Get create time of the base table. This is the last replicate time
select @table_crtime = DATEADD(mi,-30,create_date) FROM sys.objects WHERE name = @table_name and type='U'

if @error_messages_only = 'no'
Begin
	Select @time_now = (select Convert(char(8),CURRENT_TIMESTAMP, 8))
	print @time_now + ': Local table created at: ' + ISNULL(convert(varchar, @table_crtime, 20), 'does not exist')
	set @LogMessage = 'Local table created at: ' + ISNULL(convert(varchar, @table_crtime, 20), 'does not exist')
	exec SF_Logger @SPName, N'Message', @LogMessage
End

-- Get the latest timestamp from the Refresh table
select @sql = 'Select @LastTimeOUT = DATEADD(mi,-30,LastRefreshTime) from ' + @refresh_table 
select @sql = @sql + ' where TblName= ''' + @table_name + ''''
select @parmlist = '@LastTimeOUT datetime OUTPUT'
exec sp_executesql @sql,@parmlist, @LastTimeOUT=@last_time OUTPUT
IF (@@ERROR <> 0 OR @last_time is null)
begin
	set @last_time = @table_crtime
end

if (@schema_error_action = 'repair')
begin
	-- Get the latest timestamp from the local table itself
	select @sql = 'Select @LastTimeOUT = DATEADD(mi,-30,MAX(' + @timestamp_col_name + ')) from ' + @delim_table_name 
	select @parmlist = '@LastTimeOUT datetime OUTPUT'
	--print @sql
	exec sp_executesql @sql,@parmlist, @LastTimeOUT=@last_time OUTPUT
	IF (@@ERROR <> 0 OR @last_time is null)
	begin
		set @last_time = @table_crtime
	end
end

-- if the last refresh time was before the last replicate time, use the last replicate time instead
-- 150 is two hours ahead of last created time of local table, plus the 30 min backup
-- Below takes into account if there was a refresh ran while a replicate was still completing
if (@last_time < DATEADD(mi,150,@table_crtime)) and (@schema_error_action != 'repair')
   set @last_time = @table_crtime

set @last_time_local = @last_time

-- Are we using GMT locally
declare @isConvertToGMT bit
set @isConvertToGMT = 0
exec SF_IsConvertToGMT @table_server, @isConvertToGMT Output

-- If yes then convert last_time to GMT
SET @last_time_GMT = DATEADD(Hour, DATEDIFF(Hour, GETDATE(), GETUTCDATE()), @last_time)
if (@isConvertToGMT = 1)
begin
	if @error_messages_only = 'no'
	Begin
  		Select @time_now = (select Convert(char(8),CURRENT_TIMESTAMP, 8))
		print @time_now + ': DBAmp is using GMT for all datetime calculations.'
		set @last_time = @last_time_GMT
	End
end

if @error_messages_only = 'no'
Begin
	Select @time_now = (select Convert(char(8),CURRENT_TIMESTAMP, 8))
	print @time_now + ': Using last run time of ' + Convert(nvarchar(24),@last_time,120)
	set @LogMessage = 'Using last run time of ' + Convert(nvarchar(24),@last_time,120) 
	exec SF_Logger @SPName,N'Message', @LogMessage
End

-- If the delta table exists, drop it
set @delta_exist = 0;
IF EXISTS (SELECT 1
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_TYPE='BASE TABLE'
    AND TABLE_NAME=@delta_table)
        set @delta_exist = 1
IF (@@ERROR <> 0) GOTO ERR_HANDLER

if (@delta_exist = 1)
        exec ('Drop table ' + @delim_delta_table)
IF (@@ERROR <> 0) GOTO ERR_HANDLER

-- Retrieve current server name and database
select @server = @@servername, @database = DB_NAME()
SET @server = CAST(SERVERPROPERTY('ServerName') AS sysname) 

-- Create an empty local table with the current structure of the Salesforce object
Select @sql = 'Select Top 0 * into ' + @delim_delta_table + ' from ' + @table_server + '.[CData].[Salesforce].' + @delim_table_name
begin try
	exec sp_executesql @sql
end try
begin catch
	Select @time_now = (select Convert(char(8),CURRENT_TIMESTAMP, 8))
	print @time_now + ': Error when creating delta table. ' + ERROR_MESSAGE()
	set @LogMessage = 'Error when creating delta table. ' + ERROR_MESSAGE()
	exec SF_Logger @SPName, N'Message', @LogMessage 
	GOTO ERR_HANDLER
end catch

-- Remember query time as the the start of the interval
declare @queryTime datetime
select @queryTime = (Select CURRENT_TIMESTAMP)

-- Populate new delta table with updated rows	
if @bulkapi_option is null
begin
	set @sql = 'Insert ' + @delim_delta_table + ' Select * from ' + @table_server + '.[CData].[Salesforce].' + @delim_table_name + ' where ' + @timestamp_col_name + ' > ''' + Convert(nvarchar(24),@last_time,120) + ''''
	exec sp_executesql @sql
	IF (@@ERROR <> 0) GOTO ERR_HANDLER
End
else
begin
	-- Use bulkapi
	-- Execute DBAmpAZ.exe to query using the bulkapi
	set @Command = @ProgDir + 'DBAmpAZ.exe FullCopy'
	set @Command = @Command + ' "' + @delta_table + '" '
	set @Command = @Command + ' "' + @server + '" '
	set @Command = @Command + ' "' + @database + '" '
	set @Command = @Command + ' "' + @table_server + '" '
	set @Command = @Command + ' "' + Replace(@options, ' ', '') + '" '
	-- Add where clause
	set @Command = @Command + ' "where ' + RTRIM(@timestamp_col_name) +'>' + Convert(nvarchar(24),@last_time,120) + '" '

	-- Create temp table to hold output
	declare @errorlog TABLE (line varchar(255))
	insert into @errorlog
		exec @Result = master..xp_cmdshell @Command

	-- print output to msgs
	declare @line varchar(255)
	declare @printCount int
	set @printCount = 0
	DECLARE tables_cursor CURSOR FOR SELECT line FROM @errorlog
	OPEN tables_cursor
	FETCH NEXT FROM tables_cursor INTO @line
	WHILE (@@FETCH_STATUS <> -1)
	BEGIN
	   if @line is not null
		begin
   		print @line 
   		--set @errorLines = @errorLines + @line
   		set @printCount = @printCount +1	
		end
	   FETCH NEXT FROM tables_cursor INTO @line
	END
	deallocate tables_cursor


	if @Result = -1 or @printCount = 0
	BEGIN
  		Select @time_now = (select Convert(char(8),CURRENT_TIMESTAMP, 8))
		print @time_now + ': Error: DBAmpAZ.exe was unsuccessful.'
		set @LogMessage = 'Error: DBAmpAZ.exe was unsuccessful.'
		exec SF_Logger @SPName, N'Message', @LogMessage
		print @time_now + ': Error: Command string is ' + @Command
		set @LogMessage = 'Error: Command string is ' + @Command
		exec SF_Logger @SPName, N'Message', @LogMessage
  		GOTO ERR_HANDLER
	END
end


-- Delete any overlap rows in the delta table
-- These are rows we've already synched but got picked up due to the 10 min sliding window
select @sql = 'delete ' + @delim_delta_table + ' where exists '
select @sql = @sql + '(select Id from ' + @delim_table_name + ' where Id= ' + @delim_delta_table +'.Id COLLATE DATABASE_DEFAULT'
select @sql = @sql + ' and ' + @timestamp_col_name + ' = ' + @delim_delta_table + '.' + @timestamp_col_name + ')'

exec sp_executesql @sql
IF (@@ERROR <> 0) GOTO ERR_HANDLER

-- Get the count of records from the delta table
declare @delta_count int
select @sql = 'Select @DeltaCountOUT = Count(*) from ' + @delim_delta_table
select @parmlist = '@DeltaCountOUT int OUTPUT'
exec sp_executesql @sql,@parmlist, @DeltaCountOUT=@delta_count OUTPUT
IF (@@ERROR <> 0) GOTO ERR_HANDLER

-- Print number of rows in delta table
if @error_messages_only = 'no'
Begin
	Select @time_now = (select Convert(char(8),CURRENT_TIMESTAMP, 8))
	Select @char_count = (select CAST(@delta_count as VARCHAR(10)))
	print @time_now + ': Identified ' + @char_count + ' updated/inserted rows.'
	set @LogMessage = 'Identified ' + @char_count + ' updated/inserted rows.'
	exec SF_Logger @SPName, N'Message',@LogMessage
End

-- If no records have changed then move on to deletes
if (@delta_count = 0) goto DELETE_PROCESS

-- Check to see if the column structure is the same
declare @cnt1 int
declare @cnt2 int
Select @cnt1 = Count(*) FROM INFORMATION_SCHEMA.COLUMNS v1 WHERE v1.TABLE_NAME=@delta_table 
		AND NOT EXISTS (Select COLUMN_NAME from INFORMATION_SCHEMA.COLUMNS v2 
		Join INFORMATION_SCHEMA.TABLES t1
		On v2.TABLE_NAME = t1.TABLE_NAME and t1.TABLE_SCHEMA = v2.TABLE_SCHEMA
		where (t1.TABLE_TYPE = 'BASE TABLE') and
		v2.TABLE_NAME=@table_name and v1.COLUMN_NAME = v2.COLUMN_NAME 
		and (v1.DATA_TYPE = v2.DATA_TYPE or (v1.DATA_TYPE = 'nvarchar' and v2.DATA_TYPE = 'varchar') or (v1.DATA_TYPE = 'ntext' and v2.DATA_TYPE = 'nvarchar') or (v1.DATA_TYPE = 'image' and v2.DATA_TYPE = 'varbinary'))
		and (ISNULL(v1.CHARACTER_MAXIMUM_LENGTH,0) = ISNULL(v2.CHARACTER_MAXIMUM_LENGTH,0) or v1.DATA_TYPE = 'ntext' or v1.DATA_TYPE = 'image'))
IF (@@ERROR <> 0) GOTO ERR_HANDLER

Select @cnt2 = Count(*) FROM INFORMATION_SCHEMA.COLUMNS v1
		Join INFORMATION_SCHEMA.TABLES t1
		On v1.TABLE_NAME = t1.TABLE_NAME and t1.TABLE_SCHEMA = v1.TABLE_SCHEMA
		WHERE (t1.TABLE_TYPE = 'BASE TABLE') and v1.TABLE_NAME= @table_name
		AND NOT EXISTS (Select COLUMN_NAME from INFORMATION_SCHEMA.COLUMNS v2 where 
		v2.TABLE_NAME=@delta_table and v1.COLUMN_NAME = v2.COLUMN_NAME 
		and (v1.DATA_TYPE = v2.DATA_TYPE or (v1.DATA_TYPE = 'varchar' and v2.DATA_TYPE = 'nvarchar') or (v1.DATA_TYPE = 'nvarchar' and v2.DATA_TYPE = 'ntext') or (v1.DATA_TYPE = 'varbinary' and v2.DATA_TYPE = 'image'))
		and (ISNULL(v1.CHARACTER_MAXIMUM_LENGTH,0) = ISNULL(v2.CHARACTER_MAXIMUM_LENGTH,0) or v1.DATA_TYPE = 'nvarchar' or v1.DATA_TYPE = 'varbinary'))
IF (@@ERROR <> 0) GOTO ERR_HANDLER

set @diff_schema_count = @cnt1 + @cnt2
--set @diff_schema_count = 0
if (@diff_schema_count > 0)
begin
	Select @time_now = (select Convert(char(8),CURRENT_TIMESTAMP, 8))
	
		Select v1.COLUMN_NAME into #Test1 FROM INFORMATION_SCHEMA.COLUMNS v1 WHERE v1.TABLE_NAME=@delta_table 
		And v1.COLUMN_NAME Not in (Select COLUMN_NAME from INFORMATION_SCHEMA.COLUMNS v2 
		Join INFORMATION_SCHEMA.TABLES t1
		On v2.TABLE_NAME = t1.TABLE_NAME and t1.TABLE_SCHEMA = v2.TABLE_SCHEMA
		where (t1.TABLE_TYPE = 'BASE TABLE') and
		v2.TABLE_NAME=@table_name and v1.COLUMN_NAME = v2.COLUMN_NAME 
		and (v1.DATA_TYPE = v2.DATA_TYPE or (v1.DATA_TYPE = 'nvarchar' and v2.DATA_TYPE = 'varchar') or (v1.DATA_TYPE = 'ntext' and v2.DATA_TYPE = 'nvarchar') or (v1.DATA_TYPE = 'image' and v2.DATA_TYPE = 'varbinary'))
		and (ISNULL(v1.CHARACTER_MAXIMUM_LENGTH,0) = ISNULL(v2.CHARACTER_MAXIMUM_LENGTH,0) or v1.DATA_TYPE = 'ntext' or v1.DATA_TYPE = 'image'))

		Select v1.COLUMN_NAME into #Test2 FROM INFORMATION_SCHEMA.COLUMNS v1 
		Join INFORMATION_SCHEMA.TABLES t1
		On v1.TABLE_NAME = t1.TABLE_NAME and t1.TABLE_SCHEMA = v1.TABLE_SCHEMA
		WHERE (t1.TABLE_TYPE = 'BASE TABLE') and v1.TABLE_NAME=@table_name 
		AND v1.COLUMN_NAME not in (Select COLUMN_NAME from INFORMATION_SCHEMA.COLUMNS v2 where 
		v2.TABLE_NAME=@delta_table and v1.COLUMN_NAME = v2.COLUMN_NAME 
		and (v1.DATA_TYPE = v2.DATA_TYPE or (v1.DATA_TYPE = 'varchar' and v2.DATA_TYPE = 'nvarchar') or (v1.DATA_TYPE = 'nvarchar' and v2.DATA_TYPE = 'ntext') or (v1.DATA_TYPE = 'varbinary' and v2.DATA_TYPE = 'image'))
		and (ISNULL(v1.CHARACTER_MAXIMUM_LENGTH,0) = ISNULL(v2.CHARACTER_MAXIMUM_LENGTH,0) or v1.DATA_TYPE = 'nvarchar' or v1.DATA_TYPE = 'varbinary'))

	  declare @ColumnName sysname

	  declare ColumnCompare_cursor cursor 
	  for 
		 select a.COLUMN_NAME from #Test1 a

	  open ColumnCompare_cursor

	  while 1 = 1
	  begin
	  fetch next from ColumnCompare_cursor into @ColumnName
		if @@error <> 0 or @@fetch_status <> 0 break
		    begin
				print @time_now + ': Error: ' + @ColumnName + ' exists in the delta table but does not exist in the local table or has a different definition.'
				set @LogMessage = 'Error: ' + @ColumnName + ' exists in the delta table but does not exist in the local table or has a different definition.'
				exec SF_Logger @SPName, N'Message',@LogMessage
			end
	  end

	  close ColumnCompare_cursor
	  deallocate ColumnCompare_cursor

	  declare ColumnCompare2_cursor cursor 
	  for 	
	     select a.COLUMN_NAME from #Test2 a

	  open ColumnCompare2_cursor

	  while 1 = 1
	  begin
	  fetch next from ColumnCompare2_cursor into @ColumnName
		if @@error <> 0 or @@fetch_status <> 0 break
		    begin
				print @time_now + ': Error: ' + @ColumnName + ' exists in the local table but does not exist in the delta table or has a different definition.'
				set @LogMessage = 'Error: ' + @ColumnName + ' exists in the local table but does not exist in the delta table or has a different definition.'
				exec SF_Logger @SPName, N'Message',@LogMessage
			end
	  end

	  close ColumnCompare2_cursor
	  deallocate ColumnCompare2_cursor
	  
	if (@schema_error_action = 'yes') or (@schema_error_action = 'nodrop')
	begin
		if @error_messages_only = 'no'
		Begin
			print @time_now + ': Table schema has changed. The table will be replicated instead.'
			set @LogMessage = 'Table schema has changed. The table will be replicated instead.'
			exec SF_Logger @SPName, N'Message', @LogMessage
		End
		exec ('Drop table ' + @delim_delta_table)
		goto REPLICATE_EXIT
	end
   
   if (@schema_error_action = 'no')
   begin
	  print @time_now + ': Error: Table schema has changed and therefore the table cannot be refreshed.'
	  set @LogMessage = 'Error: Table schema has changed and therefore the table cannot be refreshed.'
	  exec SF_Logger @SPName, N'Message',@LogMessage
	  exec ('Drop table ' + @delim_delta_table)
  	  GOTO ERR_HANDLER
   end
   
    -- Schema changed so try to build a subset of columns
	-- Build list of columns in common
	declare colname_cursor cursor for 
		SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS v1 WHERE v1.TABLE_NAME= @table_name 
		AND EXISTS (Select COLUMN_NAME from INFORMATION_SCHEMA.COLUMNS v2 
		where v2.TABLE_NAME= @delta_table 
		and v1.COLUMN_NAME = v2.COLUMN_NAME 
		and (v1.DATA_TYPE = v2.DATA_TYPE or (v1.DATA_TYPE = 'varchar' and v2.DATA_TYPE = 'nvarchar') or (v1.DATA_TYPE = 'nvarchar' and v2.DATA_TYPE = 'ntext') or (v1.DATA_TYPE = 'varbinary' and v2.DATA_TYPE = 'image')) 
		and v1.TABLE_SCHEMA = v2.TABLE_SCHEMA
		and (ISNULL(v1.CHARACTER_MAXIMUM_LENGTH,0) = ISNULL(v2.CHARACTER_MAXIMUM_LENGTH,0) or v1.DATA_TYPE = 'nvarchar' or v1.DATA_TYPE = 'varbinary'))

	OPEN colname_cursor
	set @columnList = ''
	
	while 1 = 1
	begin
		fetch next from colname_cursor into @colname
		if @@error <> 0 or @@fetch_status <> 0 break
		set @columnList = @columnList + '[' + @colname + ']' + ','
	end
	close colname_cursor
	deallocate colname_cursor

	if Len(@columnList) = 0
	begin
		print @time_now + ': Error: Table schema has changed with no columns in common. Therefore the table cannot be refreshed.'
		set @LogMessage = 'Error: Table schema has changed with no columns in common. Therefore the table cannot be refreshed.'
		exec SF_Logger @SPName, N'Message', @LogMessage
		exec ('Drop table ' + @delim_delta_table)
		GOTO ERR_HANDLER
	end
					
	SET @columnList = SUBSTRING(@columnList, 1, Len(@columnList) - 1)

	-- Build list of columns that need to deleted in the local table
	declare colname_cursor cursor for 
		SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS v1 WHERE v1.TABLE_NAME= @table_name 
		AND not EXISTS (Select COLUMN_NAME from INFORMATION_SCHEMA.COLUMNS v2 
		where v2.TABLE_NAME= @delta_table 
		and v1.COLUMN_NAME = v2.COLUMN_NAME )

	OPEN colname_cursor
	set @deletecolumnList = ''
	while 1 = 1
	begin
		fetch next from colname_cursor into @colname
		if @@error <> 0 or @@fetch_status <> 0 break
		set @deletecolumnList = @deletecolumnList + '[' + @colname + ']' + ','
	end
	close colname_cursor
	deallocate colname_cursor

	if Len(@deletecolumnList) > 0
	begin
		SET @deletecolumnList = SUBSTRING(@deletecolumnList, 1, Len(@deletecolumnList) - 1)
	end
					
	if @error_messages_only = 'no'
	Begin
		print @time_now + ': Warning: Table schema has changed. SF_Refresh will use the valid subset of columns.'
		set @LogMessage = 'Warning: Table schema has changed. SF_Refresh will use the valid subset of columns.'
		exec SF_Logger @SPName, N'Message', @LogMessage
	End
end
else
begin
	-- Build list of columns anyway
	declare colname_cursor cursor for 
		SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS v1 WHERE v1.TABLE_NAME= @table_name 
		AND EXISTS (Select COLUMN_NAME from INFORMATION_SCHEMA.COLUMNS v2 
		where v2.TABLE_NAME= @delta_table 
		and v1.COLUMN_NAME = v2.COLUMN_NAME 
		and (v1.DATA_TYPE = v2.DATA_TYPE or (v1.DATA_TYPE = 'varchar' and v2.DATA_TYPE = 'nvarchar') or (v1.DATA_TYPE = 'nvarchar' and v2.DATA_TYPE = 'ntext') or (v1.DATA_TYPE = 'varbinary' and v2.DATA_TYPE = 'image')) 
		and v1.TABLE_SCHEMA = v2.TABLE_SCHEMA
		and (ISNULL(v1.CHARACTER_MAXIMUM_LENGTH,0) = ISNULL(v2.CHARACTER_MAXIMUM_LENGTH,0) or v1.DATA_TYPE = 'nvarchar' or v1.DATA_TYPE = 'varbinary'))

	OPEN colname_cursor
	set @columnList = ''
	
	while 1 = 1
	begin
		fetch next from colname_cursor into @colname
		if @@error <> 0 or @@fetch_status <> 0 break
		set @columnList = @columnList + '[' + @colname + ']' + ','
	end
	close colname_cursor
	deallocate colname_cursor

	SET @columnList = SUBSTRING(@columnList, 1, Len(@columnList) - 1)
End

DELETE_PROCESS:
-- Skip deleted stuff for nondeleteable tables
-- Commented out to allow correct operation for read only users


-- If the deleted table exists, drop it and recreate it
set @deleted_exist = 0;
IF EXISTS (SELECT 1
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_TYPE='BASE TABLE'
    AND TABLE_NAME=@deleted_table_ts)
        set @deleted_exist = 1
IF (@@ERROR <> 0) GOTO ERR_HANDLER

if (@deleted_exist = 1)
        exec ('Drop table ' + @delim_deleted_table)
IF (@@ERROR <> 0) GOTO ERR_HANDLER

select @sql = 'Create table ' +  @delim_deleted_table
select @sql = @sql + ' (Id nchar(18) null ) '
exec (@sql)
IF (@@ERROR <> 0) GOTO ERR_HANDLER

if (@replicateable = 1) and (@schema_error_action != 'repair') and (Lower(@table_name) != 'currencytype')
begin
	-- Object supports GetDelete api so use it to retrieve deleted ids
	select @sql = 'Insert ' + @delim_deleted_table + ' Select Id from openquery(' 
	select @sql = @sql + @table_server + ','
	select @sql = @sql + '''EXEC GetDeleted ObjectType=''''' + @actualTableName +''''','
	select @sql = @sql + ' StartDate=''''' + Convert(nvarchar(24),@last_time_local,120) + ''''''
	select @sql = @sql + ' ,EndDate=''''' + Convert(nvarchar(24),GETDATE(),120) +''''''')'

	BEGIN TRY
	   		exec (@sql)
	END TRY
	BEGIN CATCH
		Select @time_now = (select Convert(char(8),CURRENT_TIMESTAMP, 8))
		print @time_now + ': Error occurred fetching deleted rows.'	
		set @LogMessage = 'Error occurred fetching deleted rows.'
		exec SF_Logger @SPName, N'Message', @LogMessage
		print @time_now +
			': Error: ' + ERROR_MESSAGE();
		set @LogMessage = 'Error: ' + ERROR_MESSAGE()
		exec SF_Logger @SPName, N'Message', @LogMessage
		
		 -- Roll back any active or uncommittable transactions before
		 -- inserting information in the ErrorLog.
		 IF XACT_STATE() <> 0
		 BEGIN
			 ROLLBACK TRANSACTION;
		 END
		 Select @time_now = (select Convert(char(8),CURRENT_TIMESTAMP, 8))
		if (@schema_error_action = 'yes') or (@schema_error_action = 'nodrop')
		begin
			print @time_now + ': Error occurred while fetching deleted rows. The table will be replicated instead.'
			set @LogMessage = 'Error occurred while fetching deleted rows. The Table will be replicated instead.'
			exec SF_Logger @SPName, N'Message', @LogMessage
			goto REPLICATE_EXIT
		end
		else
		begin
		 goto ERR_HANDLER
		end
	END CATCH
end
else
begin
	-- Use alternate method to discover deleted id's
	if @error_messages_only = 'no'
	Begin
		Select @time_now = (select Convert(char(8),CURRENT_TIMESTAMP, 8))
		print @time_now + ': Using alternate method to determine deleted records.' 
		set @LogMessage = 'Using alternate method to determine deleted records.'
		exec SF_Logger @SPName, N'Message', @LogMessage
	End
	-- Create new deleted table for non-replicatable tables
	select @sql = 'Insert ' + @delim_deleted_table + ' Select Id from ' + @delim_table_name
	select @sql = @sql + ' where Id not in (select Id COLLATE DATABASE_DEFAULT from ' + @table_server + '.[CData].[Salesforce].' + @delim_table_name + ')'
	exec sp_executesql @sql
	IF (@@ERROR <> 0) GOTO ERR_HANDLER
end

-- Delete any rows in the deleted table that already have been deleted
-- These are rows we've already synched but got picked up due to the 10 min sliding window
select @sql = 'delete ' + @delim_deleted_table + ' where not exists '
select @sql = @sql + '(select Id from ' + @delim_table_name + ' where Id= ' + @delim_deleted_table +'.Id)'
exec sp_executesql @sql
IF (@@ERROR <> 0) GOTO ERR_HANDLER

-- Get the count of records from the deleted table
declare @deleted_count int
select @sql = 'Select @DeletedCountOUT = Count(*) from ' + @delim_deleted_table
select @parmlist = '@DeletedCountOUT int OUTPUT'
exec sp_executesql @sql,@parmlist, @DeletedCountOUT=@deleted_count OUTPUT
IF (@@ERROR <> 0) GOTO ERR_HANDLER

-- Print number of rows in deleted table
if @error_messages_only = 'no'
Begin
	Select @time_now = (select Convert(char(8),CURRENT_TIMESTAMP, 8))
	Select @char_count = (select CAST(@deleted_count as VARCHAR(10)))
	print @time_now + ': Identified ' + @char_count + ' deleted rows.'
	set @LogMessage = 'Identified ' + @char_count + ' deleted rows.'
	exec SF_Logger @SPName,N'Message', @LogMessage
End

if (@deleted_count <> 0)
begin
	-- Delete rows from local table that exist in deleted table
	if @error_messages_only = 'no'
	Begin
		Select @time_now = (select Convert(char(8),CURRENT_TIMESTAMP, 8))
		print @time_now + ': Removing deleted rows from ' + @table_name 
		set @LogMessage = 'Removing deleted rows from ' + @table_name
		exec SF_Logger @SPName, N'Message', @LogMessage
	End

	select @sql = 'delete ' + @delim_table_name + ' where Id IN (select Id from ' + @delim_deleted_table + ' )'
	exec sp_executesql @sql
	IF (@@ERROR <> 0) GOTO ERR_HANDLER
end

SKIPDELETED:
if (@delta_count > 0)
begin
	BEGIN TRAN
	if @error_messages_only = 'no'
	Begin
		Select @time_now = (select Convert(char(8),CURRENT_TIMESTAMP, 8))
		print @time_now + ': Adding updated/inserted rows into ' + @table_name 
		set @LogMessage = 'Adding updated/inserted rows into ' + @table_name
		exec SF_Logger @SPName, N'Message', @LogMessage
	End

	if @is_history_table = 0 or @table_name = 'OpportunityHistory'
	begin
		-- Delete rows from local table that exist in delta table
		-- History tables skip this step because updates are not allowed
		select @sql = 'delete ' + @delim_table_name + ' where Id IN (select Id COLLATE DATABASE_DEFAULT from ' + @delim_delta_table + ' )'
		exec sp_executesql @sql
		IF (@@ERROR <> 0) 
		begin
		   ROLLBACK
		   GOTO ERR_HANDLER
		end
	end
	
	-- Insert delta rows into local table
	if (@diff_schema_count > 0 )
	begin
		if Len(@deletecolumnList) > 0 and @schema_error_action = 'subsetdelete'
		begin
			-- Remove any deleted columns
			if @error_messages_only = 'no'
			Begin
				Select @time_now = (select Convert(char(8),CURRENT_TIMESTAMP, 8))
				print @time_now + ': Removing the following deleted columns ' + @deletecolumnList
				set @LogMessage = 'Removing the following deleted columns ' + @deletecolumnList
				exec SF_Logger @SPName, N'Message', @LogMessage
			End

			select @sql = 'alter table ' + @delim_table_name + ' drop column ' + @deletecolumnList	
			exec sp_executesql @sql
			IF (@@ERROR <> 0) 
			begin
			   ROLLBACK
			   GOTO ERR_HANDLER
			end
		end
			
		-- Now insert the new rows
		select @sql = 'insert ' + @delim_table_name + '(' + @columnList + ')' 
					+ ' select ' + @columnList + ' from ' + @delim_delta_table
		exec sp_executesql @sql
		IF (@@ERROR <> 0) 
		begin
		   ROLLBACK
		   GOTO ERR_HANDLER
		end
	end
	else
	begin
		select @sql = 'insert ' + @delim_table_name + '(' + @columnList + ')' 
					+ ' select ' + @columnList + ' from ' + @delim_delta_table
		exec sp_executesql @sql
		IF (@@ERROR <> 0) 
		begin
		   ROLLBACK
		   GOTO ERR_HANDLER
		end
	end
		
	COMMIT
end
	
SUCCESS:
-- Reset Last Refresh in the Refresh table for this object
exec ('delete ' + @refresh_table + ' where TblName =''' + @table_name + '''')
select @sql = 'insert into ' + @refresh_table + '(TblName,LastRefreshTime) Values(''' + @table_name + ''',''' + Convert(nvarchar(24),@queryTime,126) +''')'
--print @sql
exec sp_executesql @sql

-- We don't need the deleted and delta tables so drop them
exec ('Drop table ' + @delim_deleted_table)
exec ('Drop table ' + @delim_delta_table)

-- Report row count difference
declare @differ_count int
select @sql = 'Select @DifferCountOUT = Count(Id)-min(expr0) from ' + @delim_table_name
select @sql = @sql + ' ,openquery(' + @table_server + ',''Select Count(Id) from ' + @table_name + ' '')'
select @parmlist = '@DifferCountOUT int OUTPUT'

if  @verify_action <> 'no' 
begin
	exec sp_executesql @sql,@parmlist, @DifferCountOUT=@differ_count OUTPUT
	IF (@@ERROR = 0)
	Begin
	   if @differ_count <> 0 
		  begin
			Select @time_now = (select Convert(char(8),CURRENT_TIMESTAMP, 8))
			declare @action varchar(100)
		    if  @verify_action = 'warn' 
				set @action = 'Warning'
		    else set @action = 'Error'
			if @error_messages_only = 'no'
			Begin
				print @time_now + ': ' + @action + ': The row counts of the local table and salesforce differ by ' + Cast(@differ_count as varchar(10)) + ' rows.'
				set @LogMessage = @action + ': The row counts of the local table and salesforce differ by ' + Cast(@differ_count as varchar(10)) + ' rows.'
				exec SF_Logger @SPName,N'Message', @LogMessage
			End
			-- Fail the proc if user requested
			if @verify_action = 'fail'
			   RAISERROR ('--- Ending SF_Refresh. Operation FAILED.',16,1)
		  end
	End
End

print '--- Ending SF_Refresh. Operation successful.'
set @LogMessage = 'Ending - Operation Successful.' 
exec SF_Logger @SPName,N'Successful', @LogMessage
set NOCOUNT OFF
return 0

ERR_HANDLER:
-- We don't need the deleted and delta tables so drop them
IF EXISTS (SELECT 1
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_TYPE='BASE TABLE'
    AND TABLE_NAME=@deleted_table_ts)
begin
   exec ('Drop table ' + @delim_deleted_table)
end

IF EXISTS (SELECT 1
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_TYPE='BASE TABLE'
    AND TABLE_NAME=@delta_table)
begin
   exec ('Drop table ' + @delim_delta_table)
end

print('--- Ending SF_Refresh. Operation FAILED.')
set @LogMessage = 'Ending - Operation Failed.' 
exec SF_Logger @SPName,N'Failed', @LogMessage
RAISERROR ('--- Ending SF_Refresh. Operation FAILED.',16,1)
set NOCOUNT OFF
return 1

REPLICATE_EXIT:
-- We don't need the deleted and delta tables so drop them
IF EXISTS (SELECT 1
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_TYPE='BASE TABLE'
    AND TABLE_NAME=@deleted_table_ts)
begin
   exec ('Drop table ' + @delim_deleted_table)
end

IF EXISTS (SELECT 1
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_TYPE='BASE TABLE'
    AND TABLE_NAME=@delta_table)
begin
   exec ('Drop table ' + @delim_delta_table)
end

set @LogMessage = 'Ending - Branching to SF_Replicate.' 
exec SF_Logger @SPName, N'Failed',@LogMessage
If @schema_error_action = 'nodrop'
Begin
	exec SF_Replicate @table_server, @table_name, 'nodrop'
End
else
Begin
	if @usingOptions = 1
	Begin
		if @error_messages_only = 'yes'
			set @options = @options + ',erroronly'
		exec SF_Replicate @table_server, @table_name, @options
	End
	Else
	Begin
		if @error_messages_only = 'yes'
			set @options = 'erroronly'
		exec SF_Replicate @table_server, @table_name, @options
	End
End
set NOCOUNT OFF
return 0

GO

