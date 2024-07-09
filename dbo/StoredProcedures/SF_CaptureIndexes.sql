
Create PROCEDURE [dbo].[SF_CaptureIndexes]
	@table_name sysname,
	@index_string nvarchar(max) out
AS
DECLARE @IxTable SYSNAME
DECLARE @IxTableID INT
DECLARE @IxName SYSNAME
DECLARE @IxID INT
DECLARE @IgnoreDupKeys Int
set @index_string = ''

DECLARE cIX cursor local fast_forward for
    SELECT OBJECT_NAME(SI.object_id), SI.object_id, SI.name, SI.index_id, SI.ignore_dup_key
        FROM sys.indexes SI 
            LEFT JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS TC ON SI.name = TC.CONSTRAINT_NAME AND OBJECT_NAME(SI.object_id) = TC.TABLE_NAME
        WHERE TC.CONSTRAINT_NAME IS NULL
			AND OBJECT_NAME(SI.object_id) = @table_name
        ORDER BY OBJECT_NAME(SI.object_id), SI.index_id

-- Loop through all indexes
OPEN cIX

while 1 = 1
BEGIN
FETCH NEXT FROM cIX INTO @IxTable, @IxTableID, @IxName, @IxID, @IgnoreDupKeys
	if @@error <> 0 or @@fetch_status <> 0 break
	if @IxName is null
		continue

    DECLARE @IXSQL NVARCHAR(4000) SET @IXSQL = ''
    SET @IXSQL = 'CREATE '

    -- Check if the index is unique
    IF (INDEXPROPERTY(@IxTableID, @IxName, 'IsUnique') = 1)
        SET @IXSQL = @IXSQL + 'UNIQUE '
    -- Check if the index is clustered
    IF (INDEXPROPERTY(@IxTableID, @IxName, 'IsClustered') = 1)
        SET @IXSQL = @IXSQL + 'CLUSTERED '

    SET @IXSQL = @IXSQL + 'INDEX [' + @IxName + '] ON [' + @IxTable + ']('

    -- Get all columns of the index
    DECLARE cIxColumn CURSOR FOR 
        SELECT SC.name, IC.is_descending_key
        FROM sys.index_columns IC
            JOIN sys.columns SC ON IC.object_id = SC.object_id AND IC.column_id = SC.column_id
        WHERE IC.object_id = @IxTableID AND index_id = @IxID
        ORDER BY IC.index_column_id

    DECLARE @IxColumn SYSNAME
	DECLARE @isDescKey Int
    DECLARE @IxFirstColumn BIT SET @IxFirstColumn = 1
	
    -- Loop throug all columns of the index and append them to the CREATE statement
    OPEN cIxColumn
	while 1 = 1
	BEGIN
	FETCH NEXT FROM cIxColumn INTO @IxColumn, @isDescKey
		if @@error <> 0 or @@fetch_status <> 0 break

		IF (@IxFirstColumn = 1)
			SET @IxFirstColumn = 0
		ELSE
			SET @IXSQL = @IXSQL + ', '

		SET @IXSQL = @IXSQL + '[' + @IxColumn + ']'

		if @isDescKey = 1
			SET @IXSQL = @IXSQL + ' DESC'
	End
    CLOSE cIxColumn
    DEALLOCATE cIxColumn

	if @IgnoreDupKeys = 0
		SET @IXSQL = @IXSQL + '); '
	Else
		SET @IXSQL = @IXSQL + ') WITH(IGNORE_DUP_KEY = ON);'


	set @index_string = @index_string + @IXSQL
END

CLOSE cIX
DEALLOCATE cIX

GO

