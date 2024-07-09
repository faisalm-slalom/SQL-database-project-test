CREATE PROCEDURE [dbo].[SF_Logger]
	@SPName sysname,
	@Status nvarchar(20),
	@Message nvarchar(max)
AS

declare @log_table sysname
declare @delim_log_table sysname
declare @sql nvarchar(max)
declare @logCount int
declare @logMaxCount int
set @logMaxCount = 500000
set @logCount = .25*@logMaxCount
-- Comment this line to turn logging on
--return 0

declare @log_exist int

set @log_exist = 0
IF EXISTS (SELECT 1
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_TYPE='BASE TABLE' 
    AND TABLE_NAME='DBAmp_Log')
        set @log_exist = 1
IF (@@ERROR <> 0) return 0

if (@log_exist = 0)
begin
   Create Table DBAmp_Log
   (SPName sysname null,
   Status nvarchar(20) null,
   Message nvarchar(max),
   LogTime datetime null default (getdate()),
   Seen int Default 0
   )
   IF (@@ERROR <> 0) return 0
end
else
begin
	-- Check for log wrap
	-- If the log is too big, delete 1/4 of it
	if (Select COUNT(LogTime) from DBAmp_Log WITH (NOLOCK)) > @logMaxCount
	Begin
		DELETE FROM DBAmp_Log
		WHERE LogTime IN (SELECT TOP(@logCount) LogTime 
								FROM DBAmp_Log WITH (NOLOCK)
									ORDER BY LogTime asc)
	End
end

-- Add a messge to the log
SET @Message = REPLACE(@Message,'''','''''')  -- Fix issue with single quotes
Insert Into DBAmp_Log(SPName, Status, Message)
Values(Cast(@SPName as nvarchar(256)), @Status, @Message)
return 0

GO

