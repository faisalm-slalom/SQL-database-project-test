
Create PROCEDURE [dbo].[SF_RefreshAll] 
	@table_server sysname,
	@replicate_on_schema_error sysname = 'No',
	@verify_action varchar(100) = 'no',
	@allow_fail nvarchar(20) = 'no',
	@error_messages_only nvarchar(20) = 'no'
AS
-- Input Parameter @table_server - Linked Server Name
--             @replicate_on_schema_error - Controls whether to go ahead and replicate for a schema change or non refreshable table 
--                                        -    If the value is Yes then a replicate will be done for schema changes and non refreshable tables
--                                        -    If the value is Subset then a refresh with the common subset of columns will be done
--             @replicate_on_schema_error    - Controls the action for a schema change 
--                                     -    'No' : FAIL on a schema change
--                                     -    'Yes' : The table will be replicated instead
--                                     -    'Subset' : The new columns are ignored and the current
--                                                     subset of local table columns are refreshed.
--                                     -               Columns deleted on salesforce ARE NOT deleted locally. 
--                                     -    'SubsetDelete' : The new columns are ignored and the current
--                                                     subset of local table columns are refreshed.
--                                     -               Columns deleted on salesforce ARE deleted locally. 
--									   -    'Repair' :  The Max(SystemModStamp of the local table is used and 
--                                                      alternate method of handling deletes is used (slower)
--             @verify_action		   - Controls the row count compare behavior
--                                     -    'No' : Do not compare row counts
--                                     -    'Warn' : Compare row counts and issue warning if different
--                                     -    'Fail' : Compare row counts and fail the proc if different

set NOCOUNT ON
print N'--- Starting SF_RefreshAll' + ' ' +  dbo.SF_Version()
declare @LogMessage nvarchar(max)
declare @SPName nvarchar(50)
set @SPName = 'SF_RefreshAll:' + Convert(nvarchar(255), NEWID(), 20)
set @LogMessage = 'Parameters: ' + @table_server + ' ' + @replicate_on_schema_error + ' ' + @verify_action + ' Version: ' +  dbo.SF_Version()
exec SF_Logger @SPName,N'Starting', @LogMessage

declare @ProgDir varchar(250) 
set @ProgDir = 'C:\"Program Files"\CData\"CData DBAmp"\bin\'

declare @time_now char(8)
set @error_messages_only = Lower(@error_messages_only)
Select @time_now = (select Convert(char(8),CURRENT_TIMESTAMP, 8))

-- Validate parameters
if  (@error_messages_only <> 'yes' and @error_messages_only <> 'no')
begin
	print @time_now + ': Error: Invalid Error Messages Only Parameter: ' + @error_messages_only
	set @LogMessage = 'Error: Invalid Error Messages Only Parameter: ' + @error_messages_only
	exec SF_Logger @SPName, N'Message', @LogMessage
  	GOTO ERR_HANDLER
end

if (@replicate_on_schema_error = 'Yes' or @replicate_on_schema_error = 'yes')
Begin
	if @error_messages_only = 'no'
	Begin
	   print N'Warning: Replicating tables that are non-refreshable or that have schema changes.'
	   set @LogMessage = 'Warning: Replicating tables that are non-refreshable or that have schema changes.'
	   exec SF_Logger @SPName, N'Message', @LogMessage
	End
End

declare @sql nvarchar(4000)
declare @allobjects_exist int = 0
declare @table_name_all sysname = 'SF_AllObjects'
declare @server sysname
declare @database sysname
declare @Command nvarchar(4000)
declare @UsingFiles int
set @UsingFiles = 1
declare @EndingMessageThere int
set @EndingMessageThere = 0
declare @Result int
declare @tn sysname
declare @options nvarchar(255) = ''
declare @suberror int
set @suberror = 0

set @allow_fail = Lower(@allow_fail)
Select @time_now = (select Convert(char(8),CURRENT_TIMESTAMP, 8))

-- Validate parameters
if  (@allow_fail <> 'yes' and @allow_fail <> 'no')
begin
	print @time_now + ': Error: Invalid Allow Fail Parameter: ' + @allow_fail
	set @LogMessage = 'Error: Invalid Allow Fail Parameter: ' + @allow_fail
	exec SF_Logger @SPName, N'Message', @LogMessage
  	GOTO ERR_HANDLER
end

-- Create table to get all queryable objects
IF EXISTS (SELECT 1
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_TYPE='BASE TABLE' 
    AND TABLE_NAME=@table_name_all)
        set @allobjects_exist = 1
IF (@@ERROR <> 0) GOTO ERR_HANDLER

if (@allobjects_exist = 1)
        exec ('Drop table ' + @table_name_all)
IF (@@ERROR <> 0) GOTO ERR_HANDLER

exec ('Create Table ' + @table_name_all + ' (ObjectName nvarchar(500) not null)')
IF (@@ERROR <> 0) GOTO ERR_HANDLER

-- Retrieve current server name and database
select @server = @@servername, @database = DB_NAME()
SET @server = CAST(SERVERPROPERTY('ServerName') AS sysname) 

set @Command = @ProgDir + 'DBAmpAZ.exe GetAllObjects'
set @Command = @Command + ' "' + @table_name_all + '" '
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
	print @time_now + ': Error: Getting queryable objects was unsuccessful.'
	print @time_now + ': Error: Command string is ' + @Command
	
  	GOTO ERR_HANDLER
END

Create Table #tmpSF ([Name] nvarchar(500) not null)
set @sql = 'Select ObjectName from ' + @table_name_all
Insert #tmpSF EXEC (@sql)
IF (@@ERROR <> 0) GOTO ERR_HANDLER

declare tbls_cursor cursor local fast_forward
for select [Name] from #tmpSF

open tbls_cursor

while 1 = 1
begin
   fetch next from tbls_cursor into @tn
   if @@error <> 0 or @@fetch_status <> 0 break

   set @options = ''
   declare @timestampfield nvarchar(128) = null

   -- To skip tables, add a statement similiar to the statement below
   -- if @tn = 'SolutionHistory' CONTINUE
   
   -- The IdeaComment table must now be skipped.
   -- With API 24.0, sf does not allow select all rows for that table.
   if @tn = 'IdeaComment' CONTINUE
   
      -- The UserRecordAccess table must now be skipped.
   -- With API 24.0, sf does not allow select all rows for that table.
   if @tn = 'UserRecordAccess' CONTINUE
     
   -- The vote table must now be skipped.
   -- With API 17.0, sf does not allow select all rows for that table.
   if @tn = 'Vote' CONTINUE
   
   -- The ContentDocumentLink table must now be skipped.
   -- With API 21.0, sf does not allow select all rows for that table.
   if @tn = 'ContentDocumentLink' CONTINUE
 
    -- The FeedItem table must now be skipped.
   -- With API 21.0, sf does not allow select all rows for that table.
   if @tn = 'FeedItem' CONTINUE
   
    -- Skip the EventLogFile table due to size of the blobs
   if @tn = 'EventLogFile' CONTINUE

    -- Skip the EngagementHistory table because you cannot query on SystemModstamp
   if @tn = 'EngagementHistory' CONTINUE
      
   -- Skip these tables due to api restriction
   if @tn='FieldDefinition' CONTINUE
   if @tn='ListViewChartInstance' CONTINUE
   
   -- Feed tables consume huge quantities of API calls
   -- Therefore, we skip them. Comment out the lines if you would like to include them.
   if LEFT(@tn,4) = 'Feed' CONTINUE
   if RIGHT(@tn,4) = 'Feed' CONTINUE
   
   -- Skip all APEX tables becauase they have little value
   if LEFT(@tn,4) = 'Apex' CONTINUE 

   -- Knowledge _kav tables cannot handle a select without where clause so we skip them
   if RIGHT(@tn,4) = '_kav' CONTINUE
   if @tn = 'KnowledgeArticleVersion' CONTINUE
   
   if @tn = 'PlatformAction' CONTINUE
   if @tn = 'CollaborationGroupRecord' CONTINUE
   
   -- Skip offending data.com tables
   if @tn = 'DatacloudDandBCompany' CONTINUE
   if @tn = 'DcSocialProfile' CONTINUE
   if @tn = 'DataCloudConnect' CONTINUE
   if @tn = 'DatacloudCompany' CONTINUE
   if @tn = 'DatacloudContact' CONTINUE
   if @tn = 'DatacloudSocialHandle' CONTINUE
   if @tn = 'DcSocialProfileHandle' CONTINUE
   if @tn = 'DatacloudAddress' CONTINUE
   if @tn = 'OwnerChangeOptionInfo' CONTINUE
   
   if @tn = 'ContentFolderMember' CONTINUE   
   if @tn = 'EntityParticle' CONTINUE  
   if @tn = 'EntityDescription' CONTINUE 
   if @tn = 'EntityDefinition' CONTINUE 
   if @tn = 'Publisher' CONTINUE
   if @tn = 'RelationshipDomain' CONTINUE   
   if @tn = 'RelationshipInfo' CONTINUE  
   if @tn = 'ServiceFieldDataType' CONTINUE
   if @tn = 'UserEntityAccess' CONTINUE
   if @tn = 'PicklistValueInfo' CONTINUE
   if @tn = 'SearchLayout' CONTINUE
   if @tn = 'UserFieldAccess' CONTINUE
   if @tn= 'DataType' CONTINUE
   
   if @tn = 'FieldPermissions' CONTINUE
   if @tn = 'ContentFolderItem' CONTINUE
   if @tn = 'DataStatistics' CONTINUE
   if @tn = 'FlexQueueItem' CONTINUE
   if @tn = 'ContentHubItem' continue
   if @tn = 'OwnerChangeOptionInfo' continue
   if @tn = 'OutgoingEmail' continue
   if @tn = 'OutgoingEmailRelation' continue
   if @tn = 'NetworkUserHistoryRecent' continue
   if @tn = 'RecordActionHistory' continue

   	-- tables not queryable summer 2018
   if @tn = 'AppTabMember' continue
   if @tn = 'ColorDefinition' continue
   if @tn = 'IconDefinition' continue
   
   -- tables not queryable spring 2019
   if @tn = 'SiteDetail' continue
   if @tn = 'Site' continue
   if @tn = 'AccountUserTerritory2View' continue

   -- tables not queryable summer 2019
   if @tn = 'FlowVersionView' continue
   if @tn = 'FlowVariableView' continue

   -- tables not queryable winter 2021
   if @tn = 'FlowDefinitionView' continue

   --salesforce API does not support SystemModstamp
   if @tn = 'AnalyticsBotSession' continue
   if @tn = 'BotAnalytics' continue
   if @tn = 'BotEventLog' continue
   if @tn = 'OmniRoutingEventStore' continue

   -- Skip External objects because we cant select all rows
   if RIGHT(@tn,3) = '__x' CONTINUE 
   
   -- Skip big objects
   declare @isBigObject int
   set @isBigObject = 0
   exec SF_IsBigObject @tn, @isBigObject Output

   if (@isBigObject = 1)
	  continue
   
   --If table name in DBAmpTableOptions get options. If specified more than one different way, get one for actual table name.
   if exists(Select TableName from DBAmpTableOptions where @tn like TableName and SkipTable = 0) 
   Begin
	   set @options = (Select top 1 Options from DBAmpTableOptions where @tn like TableName order by TableName desc)
   End
   Else if exists(Select TableName from DBAmpTableOptions where @tn like TableName and SkipTable = 1)
   Begin
	   continue
   End

   if @error_messages_only = 'yes'
   Begin
	   if @options <> ''
		  set @options = @options + ',erroronly'
	   Else
		  set @options = 'erroronly'
   End

   Begin try
		exec SF_GetTimestampField @table_server, @tn, @timestampfield out
   End Try
   Begin Catch
		print 'Error: SF_Refresh failed for table ' + @tn
		set @LogMessage = 'Error: SF_Refresh failed for table ' + @tn
		exec SF_Logger @SPName, N'Message', @LogMessage
			print 
			'Error ' + CONVERT(VARCHAR(50), ERROR_NUMBER()) +
			', Severity ' + CONVERT(VARCHAR(5), ERROR_SEVERITY()) +
			', State ' + CONVERT(VARCHAR(5), ERROR_STATE()) + 
			', Line ' + CONVERT(VARCHAR(5), ERROR_LINE());
			print 
			ERROR_MESSAGE();
		set @LogMessage = ERROR_MESSAGE()
		set @suberror = 1
    End Catch

   if @timestampfield <> 'SystemModstamp' and @timestampfield <> 'CreatedDate' 
   begin
      -- print @tn + ' ' + @queryable
      if (@replicate_on_schema_error = 'Yes' or @replicate_on_schema_error = 'yes')
      begin
	    -- Call SF_Replicate for this table
	    begin try
			exec SF_Replicate @table_server, @tn, @options
	    end try
	    begin catch
		  print 'Error: SF_Replicate failed for table ' + @tn
		  set @LogMessage = 'Error: SF_Replicate failed for table ' + @tn
		  exec SF_Logger @SPName, N'Message', @LogMessage
			 print 
				'Error ' + CONVERT(VARCHAR(50), ERROR_NUMBER()) +
				', Severity ' + CONVERT(VARCHAR(5), ERROR_SEVERITY()) +
				', State ' + CONVERT(VARCHAR(5), ERROR_STATE()) + 
				', Line ' + CONVERT(VARCHAR(5), ERROR_LINE());
			 print 
				ERROR_MESSAGE();
			set @LogMessage = ERROR_MESSAGE()
			-- Comment out to avoid issues in log table
			--exec SF_Logger @SPName, N'Message', @LogMessage

			 -- Roll back any active or uncommittable transactions before
			 -- inserting information in the ErrorLog.
			 IF XACT_STATE() <> 0
			 BEGIN
				 ROLLBACK TRANSACTION;
			 END
		  set @suberror = 1
	    end catch
		continue
	  end
	  else
	  begin
	    -- skip this table
	    CONTINUE
	  end
   end
   
   -- Call SF_Refresh for this table
   begin try
		exec SF_Refresh @table_server, @tn , @replicate_on_schema_error, @error_messages_only, @verify_action, null, @timestampfield
   end try
   begin catch
	 print 'Error: SF_Refresh failed for table ' + @tn
	 set @LogMessage = 'Error: SF_Refresh failed for table ' + @tn
	 exec SF_Logger @SPName, N'Message', @LogMessage
	 print 
		'Error ' + CONVERT(VARCHAR(50), ERROR_NUMBER()) +
		', Severity ' + CONVERT(VARCHAR(5), ERROR_SEVERITY()) +
		', State ' + CONVERT(VARCHAR(5), ERROR_STATE()) + 
		', Line ' + CONVERT(VARCHAR(5), ERROR_LINE());
	 print 
		ERROR_MESSAGE();
		set @LogMessage = ERROR_MESSAGE()
		exec SF_Logger @SPName, N'Message', @LogMessage 
		
     -- Roll back any active or uncommittable transactions before
     -- inserting information in the ErrorLog.
     IF XACT_STATE() <> 0
     BEGIN
         ROLLBACK TRANSACTION;
     END
	 set @suberror = 1
   end catch
   
 end

close tbls_cursor
deallocate tbls_cursor

-- If one of the tables failed to replicate jump to error handler
if (@allow_fail = 'no')
Begin
	if @suberror = 1 goto ERR_HANDLER
End
else
Begin
	print N'Warning: Allow Fail parameter is set to Yes. Some tables may have failed. Check complete message output for any failures.'
End

Drop table #tmpSF
-- Turn NOCOUNT back off

print N'--- Ending SF_RefreshAll. Operation successful.'
set @LogMessage = 'Ending - Operation Successful.' 
exec SF_Logger @SPName,N'Successful', @LogMessage
set NOCOUNT OFF
return 0


ERR_HANDLER:
-- If we encounter an error creating the view, then indicate by returning 1
Drop table #tmpSF

set @LogMessage = 'Ending - Operation Failed.' 
exec SF_Logger @SPName,N'Failed', @LogMessage
-- Turn NOCOUNT back off
print N'--- Ending SF_RefreshAll. Operation failed.'
set @LogMessage = 'Ending - Operation Failed.'
exec SF_Logger @SPName, N'Failed', @LogMessage
RAISERROR ('--- Ending SF_RefreshAll. Operation FAILED.',16,1)
set NOCOUNT OFF
return 1

GO

