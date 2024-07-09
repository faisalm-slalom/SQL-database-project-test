Create PROCEDURE [dbo].SF_CreateDBAmpSettingsTable
AS
 
IF EXISTS (SELECT 1
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_TYPE='BASE TABLE'
    AND TABLE_NAME='DBAmpSettings')
begin
   Drop table DBAmpSettings
end
 
Create Table DBAmpSettings
(MinimumLongSize int, 
BulkPollingInterval int, 
BulkQueryTimeout int,   
NetworkReceiveTimeout int, 
MetadataOverride nvarchar(max),
DBAmpBooleansAsStrings bit,
DBAmpNumbersAsDecimals bit,
IncludeBinaryFieldValues bit,
ReportDateAsDateTime2 bit,
ReportTextAsVarchar bit,
ReportTimeAsNVarchar bit)
 
Insert Into DBAmpSettings (MinimumLongSize, BulkPollingInterval, BulkQueryTimeout, NetworkReceiveTimeout, MetadataOverride, DBAmpBooleansAsStrings, DBAmpNumbersAsDecimals, IncludeBinaryFieldValues, ReportDateAsDateTime2, ReportTextAsVarchar, ReportTimeAsNVarchar) 
Values(500, 60000, 60, 2400, '', 0, 0, 0, 0, 0, 0)

GO

