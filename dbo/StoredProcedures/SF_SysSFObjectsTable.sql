
Create PROCEDURE [dbo].[SF_SysSFObjectsTable] 
	@table_server sysname
AS
-- Input Parameter @table_server - Linked Server Name
print N'--- Starting SF_SysSFObjectsTable' + ' ' +  dbo.SF_Version()
set NOCOUNT ON

declare @ProgDir varchar(250) 
set @ProgDir = 'C:\"Program Files"\CData\"CData DBAmp"\bin\'

declare @table_exist int = 0
declare @options nvarchar(255) = ''
declare @table_name sysname = 'Sys_SFObjects'
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

set @options = 'systable'

-- Create table to get all queryable objects
IF EXISTS (SELECT 1
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_TYPE='BASE TABLE' 
    AND TABLE_NAME=@table_name)
        set @table_exist = 1
IF (@@ERROR <> 0) GOTO ERR_HANDLER

if (@table_exist = 1)
    exec ('Drop table ' + @table_name)
IF (@@ERROR <> 0) GOTO ERR_HANDLER

exec ('Create Table ' + @table_name + ' (ObjectName nvarchar(500) not null, Createable bit, Deletable bit, Updateable bit, Queryable bit, Replicateable bit, IsCustomField bit)')
IF (@@ERROR <> 0) GOTO ERR_HANDLER

-- Retrieve current server name and database
select @server = @@servername, @database = DB_NAME()
SET @server = CAST(SERVERPROPERTY('ServerName') AS sysname) 

set @Command = @ProgDir + 'DBAmpAZ.exe GetAllObjects'
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
	print @time_now + ': Error: Creating Sys_SFObjects table was unsuccessful.'
	print @time_now + ': Error: Command string is ' + @Command
	
  	GOTO ERR_HANDLER
END

select * from Sys_SFObjects

-- Turn NOCOUNT back off
set NOCOUNT OFF
print N'--- Ending SF_SysSFObjectsTable. Operation successful.'
return 0

ERR_HANDLER:

-- Turn NOCOUNT back off
set NOCOUNT OFF
print N'--- Ending SF_SysSFObjectsTable. Operation failed.'
RAISERROR ('--- Ending SF_SysSFObjectsTable. Operation FAILED.',16,1)
return 1

GO

