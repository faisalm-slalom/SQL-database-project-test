
Create PROCEDURE [dbo].[SF_CleanupWorkTables]

AS
print '--- Starting SF_CleanupWorkTables for ' +  dbo.SF_Version()

declare @TableName sysname

declare TableNames_cursor cursor 
for 
	Select name
    FROM sys.tables
	where (create_date < DATEADD(day, -1, GETDATE())) 
			and (name like '%_Delta%' or name like '%_Deleted%' or name like '%_Previous%')

open TableNames_cursor

while 1 = 1
begin
fetch next from TableNames_cursor into @TableName
if @@error <> 0 or @@fetch_status <> 0 break
	begin
		begin try
			exec ('Drop table ' + @TableName)
		end try
		begin catch
			print 'Error: Unable to delete ' + @TableName + ' table: ' + Error_Message()
			GOTO ERR_HANDLER
		end catch
		print 'Deleted the ' + @TableName + ' table.'
	end
end

close TableNames_cursor
deallocate TableNames_cursor

print '--- Ending SF_CleanupWorkTables. Operation successful.'
set NOCOUNT OFF
return 0

ERR_HANDLER:
close TableNames_cursor
deallocate TableNames_cursor
print '--- Ending SF_CleanupWorkTables. Operation FAILED.'
set NOCOUNT OFF
RAISERROR ('Ending SF_CleanupWorkTables. Operation FAILED.',16,1)
return 1

GO

