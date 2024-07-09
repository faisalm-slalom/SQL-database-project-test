CREATE FUNCTION  [dbo].[SF_Allow_Registry_Setting] ()
RETURNS nvarchar(10)
AS
BEGIN
    DECLARE @value nvarchar(10)
    EXEC xp_regread 'HKEY_LOCAL_MACHINE', 'SOFTWARE\CData\DBAmp\Salesforce', 'allow dbamp to run remotely', @value OUTPUT, @no_output = 'no_output'
IF @value IS NULL
BEGIN
    RETURN NULL
END 
    RETURN @value
END

GO

