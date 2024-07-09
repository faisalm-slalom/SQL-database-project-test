
Create PROCEDURE [dbo].[SF_BulkSOQL_Refresh]
	@table_server sysname,
	@table_name sysname,
	@options nvarchar(500) = null
AS
-- NOTE: This stored procedure will not work on SQL 2000.
--
-- Parameters: @table_server           - Salesforce Linked Server name (i.e. SALESFORCE)
--             @table_name             - Salesforce object to copy (i.e. Account)

-- @ProgDir - Directory containing the DBAmpNet2.exe. Defaults to the DBAmp program directory
-- If needed, modify this for your installation
declare @ProgDir   	varchar(250) 
set @ProgDir = 'C:\"Program Files"\CData\"CData DBAmp"\bin\'

declare @Command 	nvarchar(4000)
declare @Result 	int
declare @sql		nvarchar(max)
declare @time_now	char(8)
set NOCOUNT ON

print '--- Starting SF_BulkSOQL_Refresh for ' + @table_name + ' ' +  dbo.SF_Version()
declare @LogMessage nvarchar(max)
declare @SPName nvarchar(50)
set @SPName = 'SF_BulkSOQL_Refresh:' + Convert(nvarchar(255), NEWID(), 20)
set @LogMessage = 'Parameters: ' + @table_server + ' ' + @table_name
set @LogMessage = @LogMessage + ' ' + ' Version: ' +  dbo.SF_Version()
exec SF_Logger @SPName, N'Starting', @LogMessage

declare @EndingMessageThere int
set @EndingMessageThere = 0

declare @server sysname
declare @database sysname

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

declare @big_object_index int
set @big_object_index = CHARINDEX(REVERSE('__b'),REVERSE(@table_name))

if (@big_object_index = 1)
Begin
	Select @time_now = (select Convert(char(8),CURRENT_TIMESTAMP, 8))
	print @time_now + ': Error: Big Objects are not supported with SF_BulkSOQL_Refresh ' 
    set @LogMessage = 'Error: Big Objects are not supported with SF_BulkSOQL_Refresh'
    exec SF_Logger @SPName, N'Message', @LogMessage
    GOTO ERR_HANDLER
End

set @table_name = REPLACE(@table_name, ' ', '')

-- Retrieve current server name and database
select @server = @@servername, @database = DB_NAME()
SET @server = CAST(SERVERPROPERTY('ServerName') AS sysname) 

set @options = Replace(@options, ' ', '')

if @options is null or @options = ''
begin
	set @options = 'bulksoql'
end
else
begin
	set @options = @options + ',bulksoql'
end

set @Command = @ProgDir + 'DBAmpAZ.exe DeltaCopy'
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
	print @time_now + ': Error occurred running the MirrorCopy program'
	set @LogMessage = 'Error occurred running the MirrorCopy program'
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

if @@ERROR <> 0
   set @Result = -1

-- print output to msgs
declare @line varchar(255)
declare @printCount int
Set @printCount = 0

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
		exec SF_Logger @SPName,N'Message', @line
		Set @printCount = @printCount + 1
		end
	   FETCH NEXT FROM tables_cursor INTO @line
	END
	deallocate tables_cursor

if @Result = -1 or @printCount = 0 or @printCount = 1 or @EndingMessageThere = 0
BEGIN
  	Select @time_now = (select Convert(char(8),CURRENT_TIMESTAMP, 8))
	print @time_now + ': Error: MirrorCopy program was unsuccessful.'
	set @LogMessage = 'Error: MirrorCopy program was unsuccessful.'
	exec SF_Logger @SPName, N'Message', @LogMessage
	print @time_now + ': Error: Command string is ' + @Command
	set @LogMessage = 'Error: Command string is ' + @Command
	exec SF_Logger @SPName, N'Message', @LogMessage
	
  	GOTO RESTORE_ERR_HANDLER
END

print '--- Ending SF_BulkSOQL_Refresh. Operation successful.'
set @LogMessage = 'Ending - Operation Successful.' 
exec SF_Logger @SPName,N'Successful', @LogMessage
set NOCOUNT OFF
return 0

RESTORE_ERR_HANDLER:
print('--- Ending SF_Mirror. Operation FAILED.')
set @LogMessage = 'Ending - Operation Failed.' 
exec SF_Logger @SPName, N'Failed',@LogMessage
RAISERROR ('--- Ending SF_Mirror. Operation FAILED.',16,1)
set NOCOUNT OFF
return 1

ERR_HANDLER:
print('--- Ending SF_BulkSOQL_Refresh. Operation FAILED.')
set @LogMessage = 'Ending - Operation Failed.' 
exec SF_Logger @SPName,N'Failed', @LogMessage
RAISERROR ('--- Ending SF_BulkSOQL_Refresh. Operation FAILED.',16,1)
set NOCOUNT OFF
return 1

GO

