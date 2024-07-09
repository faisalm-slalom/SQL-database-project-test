
Create PROCEDURE [dbo].[SF_ReplicateIAD]
	@table_server sysname,
	@table_name sysname,
	@options	nvarchar(255) = '',
	@use_remote bit = 0
AS
-- Parameters: @table_server           - Salesforce Linked Server name (i.e. SALESFORCE)
--             @table_name             - Salesforce object to copy (i.e. Account)

-- @ProgDir - Directory containing the DBAmpAZ.exe. Defaults to the DBAmp program directory
-- If needed, modify this for your installation
declare @ProgDir   	varchar(250) 
set @ProgDir = 'C:\"Program Files"\CData\"CData DBAmp"\bin\'

declare @Result 	int = 0
declare @Command 	nvarchar(4000)
declare @time_now	char(8)
set NOCOUNT ON

print '--- Starting SF_ReplicateIAD for ' + @table_name + ' ' +  dbo.SF_Version()
declare @LogMessage nvarchar(max)
declare @SPName nvarchar(50)
set @SPName = 'SF_ReplicateIAD:' + Convert(nvarchar(255), NEWID(), 20)
set @LogMessage = 'Parameters: ' + @table_server + ' ' + @table_name + ' ' + ISNULL(@options, ' ') + ' Version: ' +  dbo.SF_Version()
Select @time_now = (select Convert(char(8),CURRENT_TIMESTAMP, 8))
print @time_now + ': ' + @LogMessage
exec SF_Logger @SPName, N'Starting',@LogMessage

set @table_name = REPLACE(@table_name, ' ', '')

declare @sql nvarchar(max)
declare @delim_table_name sysname
declare @prev_table sysname
declare @delim_prev_table sysname
declare @delete_table sysname
declare @delim_delete_table sysname
declare @server sysname
declare @database sysname
declare @EndingMessageThere int
set @EndingMessageThere = 0

declare @allow_registry_setting nvarchar(10)
set @allow_registry_setting = dbo.SF_Allow_Registry_Setting()

if (@allow_registry_setting is null or @allow_registry_setting = '')
Begin
	Select @time_now = (select Convert(char(8),CURRENT_TIMESTAMP, 8))
	print @time_now + ': Warning: could not get the allow dbamp to run remotely registry setting.' 
	set @LogMessage = 'Warning: could not get the allow dbamp to run remotely registry setting.' 
	exec SF_Logger @SPName, N'Message', @LogMessage
End

if @use_remote = 0
Begin
	if @allow_registry_setting <> 'False'
	Begin
		Select @time_now = (select Convert(char(8),CURRENT_TIMESTAMP, 8))
		print @time_now + ': Error: If you are not running DBAmp remotely, Allow DBAmp to Run Remotely must be set to False in the DBAmp Configuration Program.' 
		set @LogMessage = 'Error: If you are not running DBAmp remotely, Allow DBAmp to Run Remotely must be set to False in the DBAmp Configuration Program.' 
		exec SF_Logger @SPName, N'Message', @LogMessage
		GOTO ERR_HANDLER
	End

	declare @isTemporalTable bit
	set @isTemporalTable = 0
	exec SF_IsTemporalTable @table_name, @isTemporalTable Output

	if @isTemporalTable = 1
	Begin
		Select @time_now = (select Convert(char(8),CURRENT_TIMESTAMP, 8))
		print @time_now + ': Error: ' + @table_name + ' must not be a Temporal table in order to replicate.' 
		set @LogMessage = 'Error: ' + @table_name + ' must not be a Temporal table in order to replicate.'
		exec SF_Logger @SPName, N'Message', @LogMessage
		GOTO ERR_HANDLER
	End
End
Else
Begin
	if @allow_registry_setting <> 'True'
	Begin
		Select @time_now = (select Convert(char(8),CURRENT_TIMESTAMP, 8))
		print @time_now + ': Error: If you are running DBAmp remotely, Allow DBAmp to Run Remotely must be set to True in the DBAmp Configuration Program.' 
		set @LogMessage = 'Error: If you are running DBAmp remotely, Allow DBAmp to Run Remotely must be set to True in the DBAmp Configuration Program.' 
		exec SF_Logger @SPName, N'Message', @LogMessage
		GOTO ERR_HANDLER
	End

	Select @time_now = (select Convert(char(8),CURRENT_TIMESTAMP, 8))
	print @time_now + ': Running SF_ReplicateIAD in remote mode.' 
	set @LogMessage = 'Running SF_ReplicateIAD in remote mode.'
	exec SF_Logger @SPName, N'Message', @LogMessage
End

-- Put delimeters around names so we can name tables User, etc...
set @delim_table_name = '[' + @table_name + ']'
set @delim_prev_table = '[' + @prev_table + ']'
set @delete_table = @table_name + '_DeleteIAD'
set @delim_delete_table = '[' + @delete_table + ']'

-- Determine whether the local table exists
declare @table_exist int
declare @delete_exist int
set @table_exist = 0
set @delete_exist = 0;

IF EXISTS (SELECT 1
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_TYPE='BASE TABLE' 
    AND TABLE_NAME=@table_name)
        set @table_exist = 1
IF (@@ERROR <> 0) GOTO ERR_HANDLER

IF EXISTS (SELECT 1
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_TYPE='BASE TABLE' 
    AND TABLE_NAME=@delete_table)
        set @delete_exist = 1
IF (@@ERROR <> 0) GOTO ERR_HANDLER

if @use_remote = 0
Begin
	if @table_exist = 1
	begin
		-- Make sure that the table doesn't have any keys defined
		IF EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS
    			   WHERE CONSTRAINT_TYPE = 'FOREIGN KEY' AND TABLE_NAME=@table_name )
			begin
 		   Select @time_now = (select Convert(char(8),CURRENT_TIMESTAMP, 8))
		   print @time_now + ': Error: The table contains foreign keys and cannot be replicated.' 
		   set @LogMessage = 'Error: The table contains foreign keys and cannot be replicated.'
		   exec SF_Logger @SPName, N'Message', @LogMessage
		   GOTO ERR_HANDLER
		end
	end
End

-- If the delete table exists, drop it
Select @time_now = (select Convert(char(8),CURRENT_TIMESTAMP, 8))
print @time_now + ': Drop ' + @delete_table + ' if it exists.'
set @LogMessage = 'Drop ' + @delete_table + ' if it exists.'
exec SF_Logger @SPName, N'Message', @LogMessage
if (@delete_exist = 1)
        exec ('Drop table ' + @delim_delete_table)
IF (@@ERROR <> 0) GOTO ERR_HANDLER

set @options = Replace(@options, ' ', '')

if @options = ''
Begin
	set @options = 'soap,queryall'
End
else if @options not like '%bulkapi%' and @options not like '%pkchunk%'
Begin
	set @options = @options + ',soap,queryall'
End
Else
Begin
	set @options = @options + ',queryall'
End

-- Retrieve current server name and database
select @server = @@servername, @database = DB_NAME()
SET @server = CAST(SERVERPROPERTY('ServerName') AS sysname) 

if @use_remote = 0
Begin
	set @Command = @ProgDir + 'DBAmpAZ.exe FullCopy'
	set @Command = @Command + ' "' + @table_name + '" '
	set @Command = @Command + ' "' + @server + '" '
	set @Command = @Command + ' "' + @database + '" '
	set @Command = @Command + ' "' + @table_server + '" '
	set @Command = @Command + ' "' + Replace(@options, ' ', '') + '" '

	-- Create temp table to hold output
	declare @errorlog table (line varchar(255))

	begin try
	insert into @errorlog
		exec @Result = master..xp_cmdshell @Command
	end try
	begin catch
	   print 'Error occurred running the Replicate program'	
		Select @time_now = (select Convert(char(8),CURRENT_TIMESTAMP, 8))
		print @time_now + ': Error occurred running the Replicate program'
		set @LogMessage = 'Error occurred running the Replicate program'
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
    
	  set @Result = -1
	end catch
End
Else
Begin
	-- Create temp table to hold output
	Create Table #ErrorLog (line nvarchar(max))
	set @sql = 'Insert into #ErrorLog Exec ' + @table_server + '.[CData].[Salesforce].InvokeDBAmpAZ ''FullCopy'', ''' + @table_name + ''', ''' + @table_server + ''', ''' + @options + ''', ''' + @server + ''''

	begin try
		exec sp_executesql @sql
	end try
	begin catch
		print 'Error occurred running the DBAmpAZ program'	
		Select @time_now = (select Convert(char(8),CURRENT_TIMESTAMP, 8))
		print @time_now + ': Error occurred running the Replicate program'
		set @LogMessage = 'Error occurred running the Replicate program'
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
    
	  set @Result = -1
	end catch
End

if @@ERROR <> 0
   set @Result = -1

-- print output to msgs
declare @line varchar(255)
declare @printCount int
Set @printCount = 0

if @use_remote = 0
Begin
	DECLARE tables_cursor CURSOR FOR SELECT line FROM @errorlog
	OPEN tables_cursor
	FETCH NEXT FROM tables_cursor INTO @line
	WHILE (@@FETCH_STATUS <> -1)
	BEGIN
	   if @line is not null 
		begin
		print @line
		if CHARINDEX('Operation successful.',@line) > 0
		begin
			set @EndingMessageThere = 1
		end
		if CHARINDEX(' with new structure.', @line) > 0 and @line like '%_Previous%' and CHARINDEX('Create ', @line) > 0
		begin
			declare @createPos int
			declare @structurePos int
			declare @create nvarchar(50) = 'Create '
			declare @structure nvarchar(50) = ' with new structure.'
			set @createPos = CHARINDEX('Create ', @line)
			set @structurePos = CHARINDEX(' with new structure.', @line)
			set @prev_table = SUBSTRING(@line , @createPos + LEN(@create) + 1, (@structurePos - @createPos) - LEN(@create))
		end
		exec SF_Logger @SPName,N'Message', @line
		Set @printCount = @printCount + 1
		end
	   FETCH NEXT FROM tables_cursor INTO @line
	END
	deallocate tables_cursor
End
Else
Begin
	DECLARE tables_cursor CURSOR FOR Select Data from SF_Split((select replace(line, CHAR(13)+CHAR(10), '\r\n') from #ErrorLog), '\r\n' , 1) 
	OPEN tables_cursor
	FETCH NEXT FROM tables_cursor INTO @line
	WHILE (@@FETCH_STATUS <> -1)
	BEGIN
	   if @line is not null and RIGHT(@line, 3) <> 'r\n'
	   begin
		  if LEFT(@line, 3) = 'r\n'
		  Begin
			 set @line = STUFF(@line, 1, 3, '')
			 if CHARINDEX('Operation successful.', @line) > 0
			 begin
				 set @EndingMessageThere = 1
			 end
			 print @line
		  End
		  Else
		  Begin
			 print @line
		  End
		  exec SF_Logger @SPName,N'Message', @line
		  Set @printCount = @printCount + 1
	   End
	   FETCH NEXT FROM tables_cursor INTO @line
	END
	deallocate tables_cursor

	drop table #ErrorLog
End

if @Result = -1 or @printCount = 0 or @printCount = 1 or @EndingMessageThere = 0
BEGIN
  	Select @time_now = (select Convert(char(8),CURRENT_TIMESTAMP, 8))
	print @time_now + ': Error: Replicate program was unsuccessful.'
	set @LogMessage = 'Error: Replicate program was unsuccessful.'
	exec SF_Logger @SPName, N'Message', @LogMessage
	print @time_now + ': Error: Command string is ' + @Command
	set @LogMessage = 'Error: Command string is ' + @Command
	exec SF_Logger @SPName, N'Message', @LogMessage
	
	--Clean up any previous table
	IF EXISTS (SELECT 1
		FROM INFORMATION_SCHEMA.TABLES
		WHERE TABLE_TYPE='BASE TABLE'
		AND TABLE_NAME=@prev_table)
	begin
	   exec ('Drop table ' + @prev_table)
	end
	
  	GOTO RESTORE_ERR_HANDLER
END

print '--- Ending SF_ReplicateIAD. Operation successful.'
set @LogMessage = 'Ending - Operation Successful.' 
exec SF_Logger @SPName,N'Successful', @LogMessage
set NOCOUNT OFF
return 0

RESTORE_ERR_HANDLER:
print('--- Ending SF_ReplicateIAD. Operation FAILED.')
set @LogMessage = 'Ending - Operation Failed.' 
exec SF_Logger @SPName, N'Failed',@LogMessage
RAISERROR ('--- Ending SF_ReplicateIAD. Operation FAILED.',16,1)
set NOCOUNT OFF
return 1

ERR_HANDLER:
print('--- Ending SF_ReplicateIAD. Operation FAILED.')
set @LogMessage = 'Ending - Operation Failed.' 
exec SF_Logger @SPName,N'Failed', @LogMessage
RAISERROR ('--- Ending SF_ReplicateIAD. Operation FAILED.',16,1)
set NOCOUNT OFF
return 1

GO

