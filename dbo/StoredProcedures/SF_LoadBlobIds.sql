
Create PROCEDURE [dbo].[SF_LoadBlobIds]
	@operation nvarchar(200),
	@table_server sysname,
	@table_name sysname,
	@options nvarchar(512) = ''
AS
-- Parameters: @operation		- Operation to perform (Update, Insert, Delete)
--             @table_server           	- Salesforce Linked Server name (i.e. SALESFORCE)
--             @table_name             	- SQL Table containing ID's to delete

-- @ProgDir - Directory containing the DBAmpAZ.exe. Defaults to the DBAmp program directory
-- If needed, modify this for your installation
declare @ProgDir   	varchar(250) 
set @ProgDir = 'C:\"Program Files"\CData\"CData DBAmp"\bin\'

declare @Result 	int
declare @Command 	nvarchar(4000)
declare @time_now	char(8)
declare @errorLines varchar(max)
set @errorLines = 'SF_LoadBlobIds Error: '
set NOCOUNT ON

print '--- Starting SF_LoadBlobIds for ' + @table_name + ' ' +  dbo.SF_Version()
declare @LogMessage nvarchar(max)
declare @SPName nvarchar(50)
set @SPName = 'SF_LoadBlobIds:' + Convert(nvarchar(255), NEWID(), 20)
set @LogMessage = 'Parameters: '  + @operation + ' ' + @table_server + ' ' + @table_name + ' ' + @options + ' ' + ' Version: ' +  dbo.SF_Version()
exec SF_Logger @SPName, N'Starting',@LogMessage

declare @server sysname
declare @database sysname
declare @phrase nvarchar(100)
declare @Start int
declare @End int
declare @delim_table_name sysname
set @delim_table_name = '[' + @table_name + ']'
declare @result_table sysname
declare @result_exist int
declare @delim_result_table sysname
set @result_table = @table_name + '_Result'
set @delim_result_table = '[' + @result_table + ']'
set @operation = LOWER(@operation)
set @options = LOWER(@options)
set @options = REPLACE(@options, ' ', '')

-- Determine whether the local table and the previous copy exist
declare @table_exist int
set @table_exist = 0

IF EXISTS (SELECT 1
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_TYPE='BASE TABLE' 
    AND TABLE_NAME=@table_name)
        set @table_exist = 1
IF (@@ERROR <> 0) GOTO ERR_HANDLER

set @result_exist = 0;
IF EXISTS (SELECT 1
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_TYPE='BASE TABLE' 
    AND TABLE_NAME=@result_table)
        set @result_exist = 1
IF (@@ERROR <> 0) GOTO ERR_HANDLER

If @options like '%ignorefailures(%'
Begin
	set @Start = PATINDEX('%ignorefailures(%', @options)
	set @End = CHARINDEX(')', @options, @Start) + 1
	set @phrase = SUBSTRING(@options, @Start, @End - @Start)
	set @options = REPLACE(@options, @phrase, '')
End

If @operation <> 'update' and @operation <> 'insert'
Begin
	Select @time_now = (select Convert(char(8),CURRENT_TIMESTAMP, 8))
	print @time_now + ': Error: Insert and Update are the only operations supported.'
	set @LogMessage = 'Error: Insert and Update are the only operations supported.'
	exec SF_Logger @SPName, N'Message', @LogMessage
	set @errorLines = @errorLines + @time_now + ': Error: Insert and Update are the only operations supported.'
  	GOTO ERR_HANDLER
End

if @options = ''
	set @options = 'blobid'
Else
	set @options = @options + ',blobid'
	
-- Retrieve current server name and database
select @server = @@servername, @database = DB_NAME()
SET @server = CAST(SERVERPROPERTY('ServerName') AS sysname) 

declare @sql nvarchar(4000)
declare @parmlist nvarchar(300)

-- Execute DBAmpAZ.exe to run BulkAPI from Salesforce
set @Command = @ProgDir + 'DBAmpAZ.exe'
set @Command = @Command + ' "' + @operation + '" '
set @Command = @Command + ' "' + @table_name + '" ' 
set @Command = @Command + ' "' + @server + '" '
set @Command = @Command + ' "' + @database + '" '
set @Command = @Command + ' "' + @table_server + '" '
set @Command = @Command + ' "' + @options + '" '

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
   	exec SF_Logger @SPName,N'Message', @line
   	set @errorLines = @errorLines + @line
   	set @printCount = @printCount +1	
	end
   FETCH NEXT FROM tables_cursor INTO @line
END
deallocate tables_cursor

declare @Data nvarchar(100)
declare @Percent int
declare @PercentageOfRowsFailed decimal(18, 3)

set @Data = (Select Data
	from SF_Split(@phrase, ',', 1) 
	where Data like '%ignorefailures(%')

set @Percent = (Select SUBSTRING(@Data, CHARINDEX('(', @Data) + 1, CHARINDEX(')', @Data) - CHARINDEX('(', @Data) - 1))

If @Data like '%ignorefailures(%'
Begin
	set @Percent = @Percent
	Select @time_now = (select Convert(char(8),CURRENT_TIMESTAMP, 8))
	print @time_now + ': Allowed Failure Percent = ' + Cast(@Percent as varchar) + '.'
	set @LogMessage = 'Allowed Failure Percent = ' + Cast(@Percent as varchar) + '.'
	exec SF_Logger @SPName, N'Message', @LogMessage
End
Else
	set @Percent = '0'
	
select @parmlist = '@PercentFailed decimal(18, 3) OUTPUT'
set @sql = '(Select @PercentFailed =
(Select Cast(Sum(Case When Error not like ' + '''' + '%Operation Successful%' + '''' + ' or Error is null Then 1 Else 0 End) As decimal(18, 3)) As ErrorTotal from ' + @delim_result_table + ')' +
'/
(select Cast(Count(*) as decimal(18, 3)) As Total from ' + @delim_result_table + '))'
exec sp_executesql @sql, @parmlist, @PercentFailed=@PercentageOfRowsFailed OUTPUT

if @PercentageOfRowsFailed is not null
Begin
	set @PercentageOfRowsFailed = @PercentageOfRowsFailed*100
	Select @time_now = (select Convert(char(8),CURRENT_TIMESTAMP, 8))
	print @time_now + ': Percent Failed = ' + Cast(@PercentageOfRowsFailed as varchar) + '.'
	set @LogMessage = 'Percent Failed = ' + Cast(@PercentageOfRowsFailed as varchar) + '.'
	exec SF_Logger @SPName, N'Message', @LogMessage
End
else
Begin
	set @PercentageOfRowsFailed = 100
	Select @time_now = (select Convert(char(8),CURRENT_TIMESTAMP, 8))
	print @time_now + ': Percent Failed = ' + Cast(@PercentageOfRowsFailed as varchar) + '.'
	set @LogMessage = 'Percent Failed = ' + Cast(@PercentageOfRowsFailed as varchar) + '.'
	exec SF_Logger @SPName, N'Message', @LogMessage
End

-- If there is an error
if @Result = -1 or @printCount = 0
Begin
    -- If too many failures 
	If @PercentageOfRowsFailed > @Percent or @Percent = '0' or @PercentageOfRowsFailed is null
	Begin
		Select @time_now = (select Convert(char(8),CURRENT_TIMESTAMP, 8))
		print @time_now + ': Error: DBAmpAZ.exe was unsuccessful.'
		set @LogMessage = 'Error: DBAmpAZ.exe was unsuccessful.'
		exec SF_Logger @SPName, N'Message', @LogMessage
		print @time_now + ': Error: Command string is ' + @Command
		set @LogMessage = 'Error: Command string is ' + @Command
		exec SF_Logger @SPName, N'Message', @LogMessage
		GOTO RESTORE_ERR_HANDLER
	End
End

print '--- Ending SF_LoadBlobIds. Operation successful.'
set @LogMessage = 'Ending - Operation Successful.' 
exec SF_Logger @SPName, N'Successful',@LogMessage
set NOCOUNT OFF
return 0

RESTORE_ERR_HANDLER:

print '--- Ending SF_LoadBlobIds. Operation FAILED.'
set @LogMessage = 'Ending - Operation Failed.' 
exec SF_Logger @SPName,N'Failed', @LogMessage
set NOCOUNT OFF
RAISERROR (@errorLines,16,1)
return 1

ERR_HANDLER:

print '--- Ending SF_LoadBlobIds. Operation FAILED.'
set @LogMessage = 'Ending - Operation Failed.' 
exec SF_Logger @SPName, N'Failed',@LogMessage
set NOCOUNT OFF
RAISERROR (@errorLines,16,1)
return 1

GO

