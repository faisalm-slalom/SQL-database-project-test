
Create PROCEDURE [dbo].[SF_MirrorAll] 
	@table_server sysname,
	@allow_fail nvarchar(20) = 'no',
	@error_messages_only nvarchar(20) = 'no',
	@use_remote bit = 0
AS
-- Input Parameter @table_server - Linked Server Name
print N'--- Starting SF_MirrorAll' + ' ' +  dbo.SF_Version()
set NOCOUNT ON
declare @LogMessage nvarchar(max)
declare @SPName nvarchar(50)
set @SPName = 'SF_MirrorAll:' + Convert(nvarchar(255), NEWID(), 20)
set @LogMessage = 'Parameters: ' + @table_server + ' Version: ' +  dbo.SF_Version()
exec SF_Logger @SPName, N'Starting',@LogMessage

declare @ProgDir varchar(250) 
set @ProgDir = 'C:\"Program Files"\CData\"CData DBAmp"\bin\'

declare @allobjects_exist int = 0
declare @options nvarchar(255) = ''
declare @table_name_all sysname = 'SF_AllObjects'
declare @server sysname
declare @database sysname
declare @sql nvarchar(max)
declare @Command nvarchar(4000)
declare @time_now char(8)
declare @EndingMessageThere int
set @EndingMessageThere = 0
declare @Result int = 0

set @allow_fail = Lower(@allow_fail)
Select @time_now = (select Convert(char(8),CURRENT_TIMESTAMP, 8))

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
	print @time_now + ': Running SF_MirrorAll in remote mode.' 
	set @LogMessage = 'Running SF_MirrorAll in remote mode.'
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

-- Validate parameters
if  (@allow_fail <> 'yes' and @allow_fail <> 'no')
begin
	print @time_now + ': Error: Invalid Allow Fail Parameter: ' + @allow_fail
	set @LogMessage = 'Error: Invalid Allow Fail Parameter: ' + @allow_fail
	exec SF_Logger @SPName, N'Message', @LogMessage
  	GOTO ERR_HANDLER
end

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

-- Retrieve current server name and database
select @server = @@servername, @database = DB_NAME()
SET @server = CAST(SERVERPROPERTY('ServerName') AS sysname) 

if @use_remote = 0
Begin
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
End
Else
Begin
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

	exec ('Insert into ' + @table_name_all + ' (ObjectName) Select TABLE_NAME from ' + @table_server + '.[CData].INFORMATION_SCHEMA.tables where TABLE_TYPE = ''TABLE''') 
	IF (@@ERROR <> 0) GOTO ERR_HANDLER
End

if @@ERROR <> 0
   set @Result = -1

-- print output to msgs
declare @line varchar(255)
declare @printCount int
Set @printCount = 0

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
		if CHARINDEX('Operation successful.',@line) > 0
		begin
			set @EndingMessageThere = 1
		end
		Set @printCount = @printCount + 1
		end
	   FETCH NEXT FROM tables_cursor INTO @line
	END
	deallocate tables_cursor

	if @Result = -1 or @printCount = 0 or @printCount = 1 or @EndingMessageThere = 0
	BEGIN
  		Select @time_now = (select Convert(char(8),CURRENT_TIMESTAMP, 8))
		print @time_now + ': Error: Getting queryable objects was unsuccessful.'
		print @time_now + ': Error: Command string is ' + @Command
	
  		GOTO ERR_HANDLER
	END
End

Create Table #tmpSF ([Name] nvarchar(500) not null)
set @sql = 'Select ObjectName from ' + @table_name_all
Insert #tmpSF EXEC (@sql)
IF (@@ERROR <> 0) GOTO ERR_HANDLER

declare @tn sysname
declare @ReplicateError int
Set @ReplicateError = 0

declare tbls_cursor cursor local fast_forward
for select [Name] from #tmpSF

open tbls_cursor

while 1 = 1
begin
   fetch next from tbls_cursor into @tn
   if @@error <> 0 or @@fetch_status <> 0 break

   set @options = ''
   
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
   
   -- Skip these tables due to api restriction
   if @tn='FieldDefinition' CONTINUE
   if @tn='ListViewChartInstance' CONTINUE

   -- The ContentDocumentLink table must now be skipped.
   -- With API 21.0, sf does not allow select all rows for that table.
   if @tn = 'ContentDocumentLink' CONTINUE
 
   -- Skip the EventLogFile table due to size of the blobs
   if @tn = 'EventLogFile' CONTINUE

    -- The FeedItem table must now be skipped.
   -- With API 21.0, sf does not allow select all rows for that table.
   if @tn = 'FeedItem' CONTINUE

   -- Feed tables consume huge quantities of API calls
   -- Therefore, we skip them. Comment out the lines if you would like to include them.
   if LEFT(@tn,4) = 'Feed' CONTINUE
   if RIGHT(@tn,4) = 'Feed' CONTINUE
   
   -- Skip all APEX table because they have little use
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
   if @tn= 'ContentFolderItem' CONTINUE
   if @tn= 'DataType' CONTINUE
   
   if @tn = 'FieldPermissions' CONTINUE
   if @tn = 'DataStatistics' CONTINUE
   if @tn = 'FlexQueueItem' CONTINUE
   if @tn = 'ContentHubItem' continue
   if @tn = 'OwnerChangeOptionInfo' continue
   if @tn = 'OutgoingEmail' continue
   if @tn = 'OutgoingEmailRelation' continue
   if @tn = 'NetworkUserHistoryRecent' continue
 
	-- tables not queryable summer 2018
   if @tn = 'AppTabMember' continue
   if @tn = 'ColorDefinition' continue
   if @tn = 'IconDefinition' continue
   
    -- tables not queryable spring 2019
   if @tn = 'SiteDetail' continue
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

   --Flip to SOAP as these cannot use the BulkAPI
   if @tn = 'AcceptedEventRelation' or
   RIGHT(@tn,5) = '__Tag' or
   @tn = 'CaseStatus' or
   @tn = 'ContractStatus' or
   @tn = 'DeclinedEventRelation' or
   @tn = 'EventWhoRelation' or
   @tn = 'FieldSecurityClassification' or
   @tn = 'KnowledgeArticle' or
   @tn = 'KnowledgeArticleViewStat' or
   @tn = 'KnowledgeArticleVoteStat' or
   @tn = 'KnowledgeArticleVersionHistory' or
   @tn = 'OrderStatus' or
   @tn = 'PartnerRole' or
   @tn = 'RecentlyViewed' or
   @tn = 'SolutionStatus' or
   @tn = 'TaskPriority' or
   @tn = 'TaskStatus' or
   @tn = 'TaskWhoRelation' or
   @tn = 'UndecidedEventRelation' or
   @tn = 'ServiceAppointmentStatus' or
   @tn = 'WorkOrderLineItemStatus' or
   @tn = 'WorkOrderStatus'
   Begin
	   set @options = 'soap'
   End
	  
   if @error_messages_only = 'yes'
   Begin
	   if @options <> ''
		  set @options = @options + ',erroronly'
	   Else
		  set @options = 'erroronly'
   End 

   -- Call SF_Mirror for this table
   begin try
		exec SF_Mirror @table_server, @tn, @options, @use_remote
   end try
   begin catch
      print 'Error: SF_Mirror failed for table ' + @tn
	  set @LogMessage = 'Error: SF_Mirror failed for table ' + @tn
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
		-- exec SF_Logger @SPName, N'Message', @LogMessage 
		
     -- Roll back any active or uncommittable transactions before
     -- inserting information in the ErrorLog.
     IF XACT_STATE() <> 0
     BEGIN
         ROLLBACK TRANSACTION;
     END
     set @ReplicateError = 1
   end catch
 end

close tbls_cursor
deallocate tbls_cursor

-- If one of the tables failed to replicate jump to error handler
if (@allow_fail = 'no')
Begin
	if @ReplicateError = 1 goto ERR_HANDLER
End
else
Begin
	print N'Warning: Allow Fail parameter is set to Yes. Some tables may have failed. Check complete message output for any failures.'
End

Drop table #tmpSF

set @LogMessage = 'Ending - Operation Successful.' 
exec SF_Logger @SPName, N'Successful',@LogMessage
-- Turn NOCOUNT back off
set NOCOUNT OFF
print N'--- Ending SF_MirrorAll. Operation successful.'
return 0


ERR_HANDLER:
-- If we encounter an error, then indicate by returning 1
Drop table #tmpSF

set @LogMessage = 'Ending - Operation Failed.' 
exec SF_Logger @SPName,N'Successful', @LogMessage
-- Turn NOCOUNT back off
set NOCOUNT OFF
print N'--- Ending SF_MirrorAll. Operation failed.'
RAISERROR ('--- Ending SF_MirrorAll. Operation FAILED.',16,1)
return 1

GO

