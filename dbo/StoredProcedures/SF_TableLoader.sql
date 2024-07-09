
Create PROCEDURE [dbo].[SF_TableLoader]
	@operation nvarchar(200),
	@table_server sysname,
	@table_name sysname,
	@opt_param1	nvarchar(512) = '',
	@opt_param2 nvarchar(512) = '',
	@use_remote bit = 0
AS
-- Parameters: @operation		- Operation to perform (Update, Insert, Delete)
--             @table_server           	- Salesforce Linked Server name (i.e. SALESFORCE)
--             @table_name             	- SQL Table containing ID's to delete

-- @ProgDir - Directory containing the DBAmpAZ.exe. Defaults to the DBAmp program directory
-- If needed, modify this for your installation
declare @ProgDir   	varchar(250) 
set @ProgDir = 'C:\"Program Files"\CData\"CData DBAmp"\bin\'

declare @Result 	int = 0
declare @Command 	nvarchar(4000)
declare @time_now	char(8)
declare @errorLines varchar(max)
set @errorLines = 'SF_TableLoader Error: '
set NOCOUNT ON

print '--- Starting SF_TableLoader for ' + @table_name + ' ' +  dbo.SF_Version()
declare @LogMessage nvarchar(max)
declare @SPName nvarchar(50)
set @SPName = 'SF_TableLoader:' + Convert(nvarchar(255), NEWID(), 20)
set @LogMessage = 'Parameters: ' + @operation + ' ' + @table_server + ' ' + @table_name + ' ' + ISNULL(@opt_param1, ' ') + ' ' + ISNULL(@opt_param2, ' ') + ' Version: ' +  dbo.SF_Version()
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
declare @operationNotLower nvarchar(200)
set @operation = Replace(@operation, ' ', '')
set @operationNotLower = @operation
set @operation = lower(@operation)
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
	print @time_now + ': Running SF_TableLoader in remote mode.' 
	set @LogMessage = 'Running SF_TableLoader in remote mode.'
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

-- Determine whether the local table and the previous copy exist
declare @table_exist int
set @table_exist = 0

if Left(@table_name, 2) = '##'
Begin
    Select @time_now = (select Convert(char(8),CURRENT_TIMESTAMP, 8))
	print @time_now + ': Error: SF_TableLoader cannot be used with Global Temporary tables, use SF_BulkOps instead.'
	set @LogMessage = 'Error: SF_TableLoader cannot be used with Global Temporary tables, use SF_BulkOps instead.'
	exec SF_Logger @SPName, N'Message', @LogMessage
  	GOTO ERR_HANDLER
End

if @opt_param1 is null or @opt_param2 is null
Begin
	Select @time_now = (select Convert(char(8),CURRENT_TIMESTAMP, 8))
	print @time_now + ': Error: Optional parameters cannot be NULL.'
	set @LogMessage = 'Error: Optional parameters cannot be NULL.'
	exec SF_Logger @SPName, N'Message', @LogMessage
  	GOTO ERR_HANDLER
End

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

If @operation like '%ignorefailures(%'
Begin
	set @Start = PATINDEX('%ignorefailures(%', @operation)
	set @End = CHARINDEX(')', @operation, @Start) + 1
	set @phrase = SUBSTRING(@operation, @Start, @End - @Start)
	set @operationNotLower = REPLACE(@operation, @phrase, '')
End

if CHARINDEX('upsert',@operation) <> 0 and @opt_param1 = ' '
BEGIN
  	Select @time_now = (select Convert(char(8),CURRENT_TIMESTAMP, 8))
	print @time_now + ': Error: External Id Field Name was not provided.'
	set @LogMessage = 'Error: External Id Field Name was not provided.'
	exec SF_Logger @SPName, N'Message', @LogMessage
	set @errorLines = @errorLines + @time_now + ': Error: External Id Field Name was not provided.'
  	GOTO ERR_HANDLER
END

if CHARINDEX('upsert',@operation) <> 0 and @opt_param1 like '%,%'
BEGIN
  	Select @time_now = (select Convert(char(8),CURRENT_TIMESTAMP, 8))
	print @time_now + ': Error: External Id Field Name was not provided before the soap headers parameter.'
	set @LogMessage = 'Error: External Id Field Name was not provided before the soap headers parameter.'
	exec SF_Logger @SPName, N'Message', @LogMessage
	set @errorLines = @errorLines + @time_now + ': Error: External Id Field Name was not provided before the soap headers parameter.'
  	GOTO ERR_HANDLER
END

-- Retrieve current server name and database
select @server = @@servername, @database = DB_NAME()
SET @server = CAST(SERVERPROPERTY('ServerName') AS sysname) 

declare @sql nvarchar(4000)
declare @parmlist nvarchar(300)

declare @optionsForAZ nvarchar(500) = ''

If CHARINDEX(':', @operation) <> 0
Begin
	set @optionsForAZ = SUBSTRING(@operationNotLower, CHARINDEX(':', @operationNotLower) + 1, LEN(@operationNotLower))
	set @operationNotLower = SUBSTRING(@operationNotLower, 0 , CHARINDEX(':', @operationNotLower))
End

If LEN(@optionsForAZ) > 0
Begin
	If LEN(@opt_param1) > 0
	Begin
		if (Lower(@operationNotLower) = 'upsert')
		Begin
			if CHARINDEX('externalid(', Lower(@opt_param1)) > 0 
				set @optionsForAZ = @optionsForAZ + ',' + @opt_param1
			Else
				set @optionsForAZ = @optionsForAZ + ',externalid(' + @opt_param1 + ')'
		End
		Else
		Begin
			set @opt_param1 = Replace(@opt_param1, ',', ':')
			if CHARINDEX('soapheaders(', Lower(@opt_param1)) > 0 
				set @optionsForAZ = @optionsForAZ + ',' + @opt_param1
			Else
				set @optionsForAZ = @optionsForAZ + ',soapheaders(' + @opt_param1 + ')'
		End
	End
End
Else
Begin
	if (Lower(@operationNotLower) = 'upsert')
	Begin
		If LEN(@opt_param1) > 0
		Begin
			if CHARINDEX('externalid(', Lower(@opt_param1)) > 0 
				set @optionsForAZ = @opt_param1
			Else
				set @optionsForAZ = 'externalid(' + @opt_param1 + ')'
		End	
	End
	Else
	Begin
		If LEN(@opt_param1) > 0
		Begin
			set @opt_param1 = Replace(@opt_param1, ',', ':')
			if CHARINDEX('soapheaders(', Lower(@opt_param1)) > 0 
				set @optionsForAZ = @opt_param1
			Else
				set @optionsForAZ = 'soapheaders(' + @opt_param1 + ')'
		End
	End
End

If LEN(@optionsForAZ) > 0
Begin
	If LEN(@opt_param2) > 0
	Begin
		set @opt_param2 = Replace(@opt_param2, ',', ':')
		if CHARINDEX('soapheaders(', Lower(@opt_param2)) > 0 
			set @optionsForAZ = @optionsForAZ + ',' + @opt_param2
		Else
			set @optionsForAZ = @optionsForAZ + ',soapheaders(' + @opt_param2 + ')'
	End
End

-- print output to msgs
declare @line varchar(255)
declare @printCount int
declare @CatastrophicError bit = 0
set @printCount = 0

if @use_remote = 0
Begin
	set @Command = @ProgDir + 'DBAmpAZ.exe'
	set @Command = @Command + ' "' + @operationNotLower + '" '
	set @Command = @Command + ' "' + @table_name + '" ' 
	set @Command = @Command + ' "' + @server + '" '
	set @Command = @Command + ' "' + @database + '" '
	set @Command = @Command + ' "' + @table_server + '" '
	set @Command = @Command + ' "' + @optionsForAZ + '" '

	-- Create temp table to hold output
	declare @errorlog TABLE (line varchar(255))
	insert into @errorlog
	exec @Result = master..xp_cmdshell @Command

	DECLARE tables_cursor CURSOR FOR SELECT line FROM @errorlog
	OPEN tables_cursor
	FETCH NEXT FROM tables_cursor INTO @line
	WHILE (@@FETCH_STATUS <> -1)
	BEGIN
	   if @line is not null
		begin
   		print @line 
		if CHARINDEX('Error: ', @line) > 0
		Begin
			set @CatastrophicError = 1
		End
		if CHARINDEX('Operation successful.',@line) > 0
		begin
			set @EndingMessageThere = 1
		end
   		exec SF_Logger @SPName,N'Message', @line
   		set @errorLines = @errorLines + @line
   		set @printCount = @printCount +1	
	   end
	   FETCH NEXT FROM tables_cursor INTO @line
	END
	deallocate tables_cursor
End
Else
Begin
	-- Create temp table to hold output
	Create Table #ErrorLog (line nvarchar(max))
	set @sql = 'Insert into #ErrorLog Exec ' + @table_server + '.[CData].[Salesforce].InvokeDBAmpAZ ''' + @operationNotLower + ''', ''' + @table_name + ''', ''' + @table_server + ''', ''' + @optionsForAZ + ''', ''' + @server + ''''
	exec sp_executesql @sql

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
			 if CHARINDEX('Error: ', @line) > 0
			 begin
				 set @CatastrophicError = 1
			 end
			 if CHARINDEX('Operation successful.',@line) > 0
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
		  set @errorLines = @errorLines + @line
   		  set @printCount = @printCount + 1	
	   End
	   FETCH NEXT FROM tables_cursor INTO @line
	END
	deallocate tables_cursor

	drop table #ErrorLog
End

if @use_remote = 0
Begin
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
End

-- If there is an error
if @Result = -1 or @printCount = 0 or @EndingMessageThere = 0
Begin
	if @use_remote = 0
	Begin
		-- If too many failures 
		If (@PercentageOfRowsFailed > @Percent or @Percent = '0' or @PercentageOfRowsFailed is null or @CatastrophicError = 1) and @Percent <> '100'
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
	Else
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

print '--- Ending SF_TableLoader. Operation successful.'
set @LogMessage = 'Ending - Operation Successful.' 
exec SF_Logger @SPName, N'Successful',@LogMessage
set NOCOUNT OFF
return 0

RESTORE_ERR_HANDLER:

print '--- Ending SF_TableLoader. Operation FAILED.'
set @LogMessage = 'Ending - Operation Failed.' 
exec SF_Logger @SPName,N'Failed', @LogMessage
set NOCOUNT OFF
RAISERROR (@errorLines,16,1)
return 1

ERR_HANDLER:

print '--- Ending SF_TableLoader. Operation FAILED.'
set @LogMessage = 'Ending - Operation Failed.' 
exec SF_Logger @SPName, N'Failed',@LogMessage
set NOCOUNT OFF
RAISERROR (@errorLines,16,1)
return 1

GO

