
Create PROCEDURE [dbo].[SF_SysSFFieldsTable] 
	@table_server sysname
AS
-- Input Parameter @table_server - Linked Server Name
print N'--- Starting SF_SysSFFieldsTable' + ' ' +  dbo.SF_Version()
set NOCOUNT ON

declare @ProgDir varchar(250) 
set @ProgDir = 'C:\"Program Files"\CData\"CData DBAmp"\bin\'

declare @table_exist int = 0
declare @options nvarchar(255) = ''
declare @table_name sysname = 'Sys_SFFields'
declare @server sysname
declare @database sysname
declare @sql nvarchar(max)
declare @Command nvarchar(4000)
declare @time_now char(8)
declare @UsingFiles int
set @UsingFiles = 1
declare @EndingMessageThere int
set @EndingMessageThere = 0
declare @Result int

-- Retrieve current server name and database
select @server = @@servername, @database = DB_NAME()
SET @server = CAST(SERVERPROPERTY('ServerName') AS sysname) 

set @Command = @ProgDir + 'DBAmpAZ.exe GetAllFields'
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
    print 'Error occurred running the DBAmpAZ program'	
		
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
	Set @printCount = @printCount + 1
	end
   FETCH NEXT FROM tables_cursor INTO @line
END
deallocate tables_cursor

if @Result = -1 or @printCount = 0 or @printCount = 1 or (@EndingMessageThere = 0 and @UsingFiles = 1)
BEGIN
  	Select @time_now = (select Convert(char(8),CURRENT_TIMESTAMP, 8))
	print @time_now + ': Error: Creating Sys_SFFields table was unsuccessful.'
	print @time_now + ': Error: Command string is ' + @Command
	
  	GOTO ERR_HANDLER
END

select * from Sys_SFFields

-- Turn NOCOUNT back off
set NOCOUNT OFF
print N'--- Ending SF_SysSFFieldsTable. Operation successful.'
return 0

ERR_HANDLER:

-- Turn NOCOUNT back off
set NOCOUNT OFF
print N'--- Ending SF_SysSFFieldsTable. Operation failed.'
RAISERROR ('--- Ending SF_SysSFFieldsTable. Operation FAILED.',16,1)
return 1

GO

