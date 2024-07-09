Create PROCEDURE [dbo].[SF_BulkSOQLPrep]
	@table_server sysname,
	@table_name sysname,
	@options	nvarchar(255) = NULL,
	@soql_statement	nvarchar(max) = NULL
AS
-- Parameters: @table_server           - Salesforce Linked Server name (i.e. SALESFORCE)
--             @table_name             - Salesforce object to copy (i.e. Account)

-- @ProgDir - Directory containing the DBAmpNet2.exe. Defaults to the DBAmp program directory
-- If needed, modify this for your installation
declare @ProgDir   	varchar(250) 
set @ProgDir = 'C:\"Program Files"\CData\"CData DBAmp"\bin\'

declare @Result 	int
declare @Command 	nvarchar(4000)
declare @time_now	char(8)
set NOCOUNT ON

print '--- Starting SF_BulkSOQLPrep for ' + @table_name + ' ' +  dbo.SF_Version()
declare @LogMessage nvarchar(max)
declare @SPName nvarchar(50)
set @SPName = 'SF_BulkSOQLPrep:' + Convert(nvarchar(255), NEWID(), 20)
set @LogMessage = 'Parameters: ' + @table_server + ' ' + @table_name + ' ' + ISNULL(@options, ' ') + ' Version: ' +  dbo.SF_Version()
Select @time_now = (select Convert(char(8),CURRENT_TIMESTAMP, 8))
print @time_now + ': ' + @LogMessage
exec SF_Logger @SPName, N'Starting',@LogMessage

declare @delim_table_name sysname
declare @result_table sysname
declare @delim_result_table sysname
declare @server sysname
declare @database sysname
declare @SOQLTable sysname
declare @UsingFiles int
set @UsingFiles = 0
declare @EndingMessageThere int
set @EndingMessageThere = 0

-- Put delimeters around names so we can name tables User, etc...
set @delim_table_name = '[' + @table_name + ']'
set @result_table = @table_name + '_Result'
set @delim_result_table = '[' + @result_table + ']'
set @SOQLTable = @table_name + '_SOQL'

-- Determine whether the local table and the previous copy exist
declare @table_exist int
declare @result_exist int
declare @soqltable_exist int
declare @SOQLStatement nvarchar(max)
declare @TempSOQLStatement nvarchar(max)
declare @sql nvarchar(1000)
declare @ParmDefinition nvarchar(500)
set @soqltable_exist = 0

IF EXISTS (SELECT 1
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_TYPE='BASE TABLE' 
    AND TABLE_NAME=@SOQLTable)
        set @soqltable_exist = 1
IF (@@ERROR <> 0) GOTO ERR_HANDLER

if @soql_statement is not null
Begin
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
set @result_exist = 0;
IF EXISTS (SELECT 1
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_TYPE='BASE TABLE' 
    AND TABLE_NAME=@table_name)
        set @table_exist = 1
IF (@@ERROR <> 0) GOTO ERR_HANDLER

IF EXISTS (SELECT 1
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_TYPE='BASE TABLE' 
    AND TABLE_NAME=@result_table)
        set @result_exist = 1
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

-- If the previous table exists, drop it
Select @time_now = (select Convert(char(8),CURRENT_TIMESTAMP, 8))
print @time_now + ': Drop ' + @result_table + ' if it exists.'
set @LogMessage = 'Drop ' + @result_table + ' if it exists.'
exec SF_Logger @SPName, N'Message', @LogMessage
if (@result_exist = 1)
        exec ('Drop table ' + @delim_result_table)
IF (@@ERROR <> 0) GOTO ERR_HANDLER

-- Create an empty local table with the current structure of the Salesforce object
Select @time_now = (select Convert(char(8),CURRENT_TIMESTAMP, 8))
print @time_now + ': Create ' + @result_table + ' with new structure.'
set @LogMessage = 'Create ' + @result_table + ' with new structure.'
exec SF_Logger @SPName, N'Message', @LogMessage

set @TempSOQLStatement = REPLACE(@SOQLStatement, '''', '''''')

-- Create previous table from _SOQL table
begin try
exec ('Select * into ' + @delim_result_table + ' from openquery(' + @table_server + ', ' + '''' + @TempSOQLStatement + '''' + ') where 1=0')
end try
begin catch
	Select @time_now = (select Convert(char(8),CURRENT_TIMESTAMP, 8))
	print @time_now + ': Error: Could not create result table.'
	set @LogMessage = 'Error: Could not create result table.'
	exec SF_Logger @SPName, N'Message', @LogMessage
	print @time_now +
		': Error: ' + ERROR_MESSAGE();
	set @LogMessage = 'Error: ' + ERROR_MESSAGE()
	exec SF_Logger @SPName, N'Message', @LogMessage
	GOTO ERR_HANDLER
end catch

print '--- Ending SF_BulkSOQLPrep. Operation successful.'
set @LogMessage = 'Ending - Operation Successful.' 
exec SF_Logger @SPName,N'Successful', @LogMessage
set NOCOUNT OFF
return 0

RESTORE_ERR_HANDLER:
print('--- Ending SF_BulkSOQLPrep. Operation FAILED.')
set @LogMessage = 'Ending - Operation Failed.' 
exec SF_Logger @SPName, N'Failed',@LogMessage
RAISERROR ('--- Ending SF_BulkSOQLPrep. Operation FAILED.',16,1)
set NOCOUNT OFF
return 1

ERR_HANDLER:
print('--- Ending SF_BulkSOQLPrep. Operation FAILED.')
set @LogMessage = 'Ending - Operation Failed.' 
exec SF_Logger @SPName,N'Failed', @LogMessage
RAISERROR ('--- Ending SF_BulkSOQLPrep. Operation FAILED.',16,1)
set NOCOUNT OFF
return 1

GO

