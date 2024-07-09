
Create PROCEDURE [dbo].[SF_IsBigObject] 
@table_name NVARCHAR(MAX), 
@IsBigObject Int OUTPUT

AS

set @IsBigObject = 0

declare @big_object_index int
declare @custom_object_index int

set @big_object_index = CHARINDEX(REVERSE('__b'),REVERSE(@table_name))
set @custom_object_index = CHARINDEX(REVERSE('__c'),REVERSE(@table_name))

if @big_object_index <> 0
Begin
	if((@big_object_index < @custom_object_index and @custom_object_index <> '0') or (@big_object_index > @custom_object_index and @custom_object_index = '0')) 
	Begin
		set @IsBigObject = 1
	end
END

GO

