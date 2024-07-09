
CREATE PROCEDURE [dbo].[SF_BulkSOQL]
	@table_server sysname,
	@table_name sysname,
	@options	nvarchar(255) = NULL,
	@soql_statement	nvarchar(max) = NULL,
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

--Get rid of any spaces in table name parameter
set @table_name = REPLACE(@table_name, ' ', '')

print '--- Starting SF_BulkSOQL for ' + @table_name + ' ' +  dbo.SF_Version()
declare @LogMessage nvarchar(max)
declare @SPName nvarchar(50)
set @SPName = 'SF_BulkSOQL:' + Convert(nvarchar(255), NEWID(), 20)
set @LogMessage = 'Parameters: ' + @table_server + ' ' + @table_name + ' ' + ISNULL(@options, ' ') + ' Version: ' +  dbo.SF_Version()
Select @time_now = (select Convert(char(8),CURRENT_TIMESTAMP, 8))
print @time_now + ': ' + @LogMessage
exec SF_Logger @SPName, N'Starting',@LogMessage

declare @delim_table_name sysname
declare @server sysname
declare @database sysname
declare @SOQLTable sysname
declare @UsingFiles int
set @UsingFiles = 0
declare @EndingMessageThere int
set @EndingMessageThere = 0
declare @UsingSOQLStatement int
set @UsingSOQLStatement = 0
declare @line varchar(255)
declare @printCount int
Set @printCount = 0

-- Put delimeters around names so we can name tables User, etc...
set @delim_table_name = '[' + @table_name + ']'
set @SOQLTable = @table_name + '_SOQL'

-- Determine whether the local table and the previous copy exist
declare @table_exist int
declare @soqltable_exist int
declare @SOQLStatement nvarchar(max)
declare @sql nvarchar(max)
declare @ParmDefinition nvarchar(500)
set @soqltable_exist = 0

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
	print @time_now + ': Running SF_BulkSOQL in remote mode.' 
	set @LogMessage = 'Running SF_BulkSOQL in remote mode.'
	exec SF_Logger @SPName, N'Message', @LogMessage
End

-- Retrieve current server name and database
select @server = @@servername, @database = DB_NAME()
SET @server = CAST(SERVERPROPERTY('ServerName') AS sysname) 

if @use_remote = 0
Begin
	IF EXISTS (SELECT 1
		FROM INFORMATION_SCHEMA.TABLES
		WHERE TABLE_TYPE='BASE TABLE' 
		AND TABLE_NAME=@SOQLTable)
			set @soqltable_exist = 1
	IF (@@ERROR <> 0) GOTO ERR_HANDLER

	if @soql_statement is not null
	Begin
		set @UsingSOQLStatement = 1
	
		-- If the SOQL table exists, drop it
		Select @time_now = (select Convert(char(8),CURRENT_TIMESTAMP, 8))
		print @time_now + ': Drop ' + @SOQLTable + ' if it exists.'
		set @LogMessage = 'Drop ' + @SOQLTable + ' if it exists.'
		exec SF_Logger @SPName, N'Message', @LogMessage
		if @soqltable_exist = 1
		Begin
			exec ('Drop table ' + @SOQLTable)
			IF (@@ERROR <> 0) GOTO ERR_HANDLER
		End

		-- Create SOQL table
		Select @time_now = (select Convert(char(8),CURRENT_TIMESTAMP, 8))
		print @time_now + ': Creating table ' + @SOQLTable + '.'
		set @LogMessage = 'Creating table ' + @SOQLTable + '.'
		exec SF_Logger @SPName, N'Message', @LogMessage
		set @sql = 'CREATE TABLE ' + @SOQLTable + ' (SOQL nvarchar(max))'
		EXECUTE sp_executesql @sql
		set @soqltable_exist = 1
	
		-- Populate SOQL table
		Select @time_now = (select Convert(char(8),CURRENT_TIMESTAMP, 8))
		print @time_now + ': Populating ' + @SOQLTable + ' with SOQL statement.'
		set @LogMessage = 'Populating ' + @SOQLTable + ' with SOQL statement.'
		exec SF_Logger @SPName, N'Message', @LogMessage
		set @sql = 'Insert Into ' + @SOQLTable + '(SOQL) values (''' + REPLACE(@soql_statement, '''', '''''') + ''')'
		EXECUTE sp_executesql @sql;
	End

	if @soqltable_exist = 0
	Begin
	   Select @time_now = (select Convert(char(8),CURRENT_TIMESTAMP, 8))
	   print @time_now + ': Error: The ' + @SOQLTable + ' table does not exist. Create an ' + @SOQLTable + ' table and populate it with a valid SOQL statement.'
	   set @LogMessage = 'Error: The ' + @SOQLTable + ' table does not exist. Create an ' + @SOQLTable + ' table and populate it with a valid SOQL statement.'
	   exec SF_Logger @SPName, N'Message', @LogMessage
	   GOTO ERR_HANDLER
	End

	select @sql = N'select @SOQLStatementOut = SOQL from ' + @table_name + '_SOQL'
	set @ParmDefinition = N'@SOQLStatementOut nvarchar(max) OUTPUT'
	exec sp_executesql @sql, @ParmDefinition, @SOQLStatementOut = @SOQLStatement OUTPUT 

	if @SOQLStatement is null or @SOQLStatement = ''
	Begin
	   Select @time_now = (select Convert(char(8),CURRENT_TIMESTAMP, 8))
	   print @time_now + ': Error: The SOQL Statement provided does not exist. Populate the SOQL column with a valid SOQL Statement.'
	   set @LogMessage = 'Error: The SOQL Statement provided does not exist. Populate the SOQL column with a valid SOQL Statement.'
	   exec SF_Logger @SPName, N'Message', @LogMessage
	   GOTO ERR_HANDLER
	End

	set @table_exist = 0

	IF EXISTS (SELECT 1
		FROM INFORMATION_SCHEMA.TABLES
		WHERE TABLE_TYPE='BASE TABLE' 
		AND TABLE_NAME=@table_name)
			set @table_exist = 1
	IF (@@ERROR <> 0) GOTO ERR_HANDLER

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
Else
Begin
	if @soql_statement is null or @soql_statement = ''
	Begin
	   Select @time_now = (select Convert(char(8),CURRENT_TIMESTAMP, 8))
	   print @time_now + ': Error: A SOQL Statement must be provided when running SF_BulkSOQL in remote mode. Populate the SOQL Statement parameter with a valid SOQL Statement.'
	   set @LogMessage = 'Error: A SOQL Statement must be provided when running SF_BulkSOQL in remote mode. Populate the SOQL Statement parameter with a valid SOQL Statement.'
	   exec SF_Logger @SPName, N'Message', @LogMessage
	   GOTO ERR_HANDLER
	End
	Else
	Begin
		-- Create temp table to hold output
		Create Table #ErrorLog (line nvarchar(max))
		set @sql = 'Insert into #ErrorLog Exec ' + @table_server + '.[CData].[Salesforce].InvokeDBAmpAZ ''CreateSOQLTable:' + @soql_statement + ''', ''' + @table_name + ''', ''' + @table_server + ''', ''' + @options + ''', ''' + @server + ''''

		begin try
			exec sp_executesql @sql
		end try
		begin catch
			print 'Error occurred running the DBAmpAZ program'	
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

		if @Result = -1 or @printCount = 0 or @printCount = 1 or @EndingMessageThere = 0
		BEGIN
  			Select @time_now = (select Convert(char(8),CURRENT_TIMESTAMP, 8))
			print @time_now + ': Error: DBAmpAZ.exe was unsuccessful.'
			set @LogMessage = 'Error: DBAmpAZ.exe was unsuccessful.'
			exec SF_Logger @SPName, N'Message', @LogMessage
  			GOTO RESTORE_ERR_HANDLER
		END
	End
End

set @EndingMessageThere = 0
Set @printCount = 0

set @options = Replace(@options, ' ', '')

if @options is null or @options = ''
begin
	set @options = 'bulksoql'
end
else
begin
	set @options = @options + ',bulksoql'
end

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
	Create Table #ErrorLog2 (line nvarchar(max))
	set @sql = 'Insert into #ErrorLog2 Exec ' + @table_server + '.[CData].[Salesforce].InvokeDBAmpAZ ''FullCopy'', ''' + @table_name + ''', ''' + @table_server + ''', ''' + @options + ''', ''' + @server + ''''

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
	DECLARE tables_cursor CURSOR FOR Select Data from SF_Split((select replace(line, CHAR(13)+CHAR(10), '\r\n') from #ErrorLog2), '\r\n' , 1) 
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

	drop table #ErrorLog2
End

if @Result = -1 or @printCount = 0 or @printCount = 1 or (@EndingMessageThere = 0 and @UsingFiles = 1)
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

print '--- Ending SF_BulkSOQL. Operation successful.'
set @LogMessage = 'Ending - Operation Successful.' 
exec SF_Logger @SPName,N'Successful', @LogMessage
set NOCOUNT OFF
return 0

RESTORE_ERR_HANDLER:
print('--- Ending SF_BulkSOQL. Operation FAILED.')
set @LogMessage = 'Ending - Operation Failed.' 
exec SF_Logger @SPName, N'Failed',@LogMessage
RAISERROR ('--- Ending SF_BulkSOQL. Operation FAILED.',16,1)
set NOCOUNT OFF
return 1

ERR_HANDLER:
print('--- Ending SF_BulkSOQL. Operation FAILED.')
set @LogMessage = 'Ending - Operation Failed.' 
exec SF_Logger @SPName,N'Failed', @LogMessage
RAISERROR ('--- Ending SF_BulkSOQL. Operation FAILED.',16,1)
set NOCOUNT OFF
return 1

GO

