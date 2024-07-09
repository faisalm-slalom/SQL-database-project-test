
CREATE PROCEDURE [dbo].[SF_DownloadBlobs]
	@table_server sysname,
	@table_name sysname,
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

print '--- Starting SF_DownloadBlobs for ' + @table_name + ' ' +  dbo.SF_Version()
declare @LogMessage nvarchar(max)
declare @SPName nvarchar(50)
set @SPName = 'SF_DownloadBlobs:' + Convert(nvarchar(255), NEWID(), 20)
set @LogMessage = 'Parameters: ' + @table_server + ' ' + @table_name + ' ' + ' Version: ' +  dbo.SF_Version()
Select @time_now = (select Convert(char(8),CURRENT_TIMESTAMP, 8))
print @time_now + ': ' + @LogMessage
exec SF_Logger @SPName, N'Starting',@LogMessage

declare @delim_table_name sysname
declare @server sysname
declare @database sysname
declare @EndingMessageThere int
set @EndingMessageThere = 0
declare @options nvarchar(255) = ''
declare @sql nvarchar(max)

-- Put delimeters around names so we can name tables User, etc...
set @delim_table_name = '[' + @table_name + ']'

-- Determine whether the local table and the previous copy exist
declare @table_exist int
declare @prev_exist int

set @table_exist = 0
set @prev_exist = 0;

declare @allow_registry_setting nvarchar(10)
set @allow_registry_setting = dbo.SF_Allow_Registry_Setting()

if (@allow_registry_setting is null or @allow_registry_setting = '')
Begin
	Select @time_now = (select Convert(char(8),CURRENT_TIMESTAMP, 8))
	print @time_now + ': Warning: could not get the allow dbamp to run remotely registry setting.' 
	set @LogMessage = 'Warning: could not get the allow dbamp to run remotely registry setting.' 
	exec SF_Logger @SPName, N'Message', @LogMessage
End

if @use_remote = 1
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
	print @time_now + ': Running SF_Metadata in remote mode.' 
	set @LogMessage = 'Running SF_Metadata in remote mode.'
	exec SF_Logger @SPName, N'Message', @LogMessage
End
Else
Begin
	if @allow_registry_setting <> 'False'
	Begin
		Select @time_now = (select Convert(char(8),CURRENT_TIMESTAMP, 8))
		print @time_now + ': Error: If you are not running DBAmp remotely, Allow DBAmp to Run Remotely must be set to False in the DBAmp Configuration Program.' 
		set @LogMessage = 'Error: If you are not running DBAmp remotely, Allow DBAmp to Run Remotely must be set to False in the DBAmp Configuration Program.' 
		exec SF_Logger @SPName, N'Message', @LogMessage
		GOTO ERR_HANDLER
	End
End

-- Retrieve current server name and database
select @server = @@servername, @database = DB_NAME()
SET @server = CAST(SERVERPROPERTY('ServerName') AS sysname) 

if @use_remote = 0
Begin
	set @Command = @ProgDir + 'DBAmpAZ.exe DownloadBlobs' 
	set @Command = @Command + ' "' + @table_name + '" '
	set @Command = @Command + ' "' + @server + '" '
	set @Command = @Command + ' "' + @database + '" '
	set @Command = @Command + ' "' + @table_server + '" '
	set @Command = @Command + ' "' + @options + '" '

	-- Create temp table to hold output
	declare @errorlog table (line varchar(255))

	begin try
		insert into @errorlog
		exec @Result = master..xp_cmdshell @Command
	end try
	begin catch
	   print 'Error occurred running the DBAmpAZ.exe program'	
		Select @time_now = (select Convert(char(8),CURRENT_TIMESTAMP, 8))
		print @time_now + ': Error occurred running the DBAmpAZ.exe program'
		set @LogMessage = 'Error occurred running the DBAmpAZ.exe program'
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
	set @sql = 'Insert into #ErrorLog Exec ' + @table_server + '.[CData].[Salesforce].InvokeDBAmpAZ ''DownloadBlobs'', ''' + @table_name + ''', ''' + @table_server + ''', ''' + @options + ''', ''' + @server + ''''

	begin try
		exec sp_executesql @sql
	end try
	begin catch
		print 'Error occurred running the DBAmpAZ program'	
		Select @time_now = (select Convert(char(8),CURRENT_TIMESTAMP, 8))
		print @time_now + ': Error occurred running the DBAmpAZ.exe program'
		set @LogMessage = 'Error occurred running the DBAmpAZ.exe program'
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
		if CHARINDEX('DBAmpAZ Operation successful.',@line) > 0
		begin
			set @EndingMessageThere = 1
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
	print @time_now + ': Error: DBAmpAZ.exe was unsuccessful.'
	set @LogMessage = 'Error: DBAmpAZ.exe was unsuccessful.'
	exec SF_Logger @SPName, N'Message', @LogMessage
	print @time_now + ': Error: Command string is ' + @Command
	set @LogMessage = 'Error: Command string is ' + @Command
	exec SF_Logger @SPName, N'Message', @LogMessage
  	GOTO RESTORE_ERR_HANDLER
END

print '--- Ending SF_DownloadBlobs. Operation successful.'
set @LogMessage = 'Ending - Operation Successful.' 
exec SF_Logger @SPName,N'Successful', @LogMessage
set NOCOUNT OFF
return 0

RESTORE_ERR_HANDLER:
print('--- Ending SF_DownloadBlobs. Operation FAILED.')
set @LogMessage = 'Ending - Operation Failed.' 
exec SF_Logger @SPName, N'Failed',@LogMessage
RAISERROR ('--- Ending SF_DownloadBlobs. Operation FAILED.',16,1)
set NOCOUNT OFF
return 1

ERR_HANDLER:
print('--- Ending SF_DownloadBlobs. Operation FAILED.')
set @LogMessage = 'Ending - Operation Failed.' 
exec SF_Logger @SPName,N'Failed', @LogMessage
RAISERROR ('--- Ending SF_DownloadBlobs. Operation FAILED.',16,1)
set NOCOUNT OFF
return 1

GO

