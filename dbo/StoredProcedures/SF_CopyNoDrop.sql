Create PROCEDURE [dbo].[SF_CopyNoDrop]
	@prev_table sysname,
	@table_name sysname
AS

declare @sql nvarchar(max)
declare @DropColumnName nvarchar(100)
declare @AddColumnName nvarchar(100)
declare @DataType nvarchar(100)
declare @DataTypeLength bigint
declare @NumericScale int
declare @ColumnList nvarchar(max)
declare @delim_table_name sysname
declare @diff_schema_count int
declare @NullVar nvarchar(50)

set @delim_table_name = '[' + @table_name + ']'


-- Delete current rows in local table
-- If not using transactional replication then you could 
--    switch to the truncate
--set @sql = 'truncate table ' + @delim_table_name
set @sql = 'delete ' + @delim_table_name
exec sp_executesql @sql


--Drop all columns, except Id, from local table
DECLARE DropColumnsLocalTable CURSOR Local Fast_Forward FOR
	Select c1.COLUMN_NAME, c1.DATA_TYPE, c1.IS_NULLABLE, c1.CHARACTER_MAXIMUM_LENGTH, c1.NUMERIC_SCALE
	FROM INFORMATION_SCHEMA.COLUMNS c1, INFORMATION_SCHEMA.TABLES t1
	WHERE c1.TABLE_NAME=@table_name and t1.TABLE_NAME = c1.TABLE_NAME and t1.TABLE_TYPE = 'BASE TABLE'
	EXCEPT
	Select c1.COLUMN_NAME, c1.DATA_TYPE, c1.IS_NULLABLE, c1.CHARACTER_MAXIMUM_LENGTH, c1.NUMERIC_SCALE
	FROM INFORMATION_SCHEMA.COLUMNS c1, INFORMATION_SCHEMA.TABLES t1
	WHERE c1.TABLE_NAME=@prev_table and t1.TABLE_NAME = c1.TABLE_NAME and t1.TABLE_TYPE = 'BASE TABLE'
OPEN DropColumnsLocalTable

While 1=1
Begin
	FETCH NEXT FROM DropColumnsLocalTable into @DropColumnName, @DataType, @NullVar, @DataTypeLength, @NumericScale
	if @@error <> 0 or @@fetch_status <> 0 break
	Begin
		set @sql = 'Alter Table ' + @delim_table_name + ' Drop Column ' + @DropColumnName
		--print 'Dropping ' + @DropColumnName
		exec sp_executesql @sql
	End
end
close DropColumnsLocalTable
deallocate DropColumnsLocalTable

--Add all columns, except Id, from previous table to local table
DECLARE AddColumnsLocalTable CURSOR Local Fast_Forward FOR
	Select c1.COLUMN_NAME, c1.DATA_TYPE, c1.IS_NULLABLE, c1.CHARACTER_MAXIMUM_LENGTH, c1.NUMERIC_SCALE
	FROM INFORMATION_SCHEMA.COLUMNS c1, INFORMATION_SCHEMA.TABLES t1
	WHERE c1.TABLE_NAME=@prev_table and t1.TABLE_NAME = c1.TABLE_NAME and t1.TABLE_TYPE = 'BASE TABLE'
	EXCEPT
	Select c1.COLUMN_NAME, c1.DATA_TYPE, c1.IS_NULLABLE, c1.CHARACTER_MAXIMUM_LENGTH, c1.NUMERIC_SCALE
	FROM INFORMATION_SCHEMA.COLUMNS c1, INFORMATION_SCHEMA.TABLES t1
	WHERE c1.TABLE_NAME=@table_name and t1.TABLE_NAME = c1.TABLE_NAME and t1.TABLE_TYPE = 'BASE TABLE'
OPEN AddColumnsLocalTable

While 1=1
Begin
	FETCH NEXT FROM AddColumnsLocalTable into @AddColumnName, @DataType, @NullVar, @DataTypeLength, @NumericScale
	if @@error <> 0 or @@fetch_status <> 0 break
	If @NullVar = 'Yes'
		Set @NullVar = 'NULL'
	Else
		Set @NullVar = 'NOT NULL'
	
	If @DataTypeLength is not null and @DataType = 'ntext'
	Begin
		set @sql = 'Alter Table ' + @delim_table_name + ' Add ' + @AddColumnName + ' ntext ' + @NullVar
	End
	Else If @DataTypeLength is not null and @DataTypeLength = -1
	Begin
		set @sql = 'Alter Table ' + @delim_table_name + ' Add ' + @AddColumnName + ' nvarchar(max) ' + @NullVar
	End
	Else If @DataTypeLength is not null
	Begin
		set @sql = 'Alter Table ' + @delim_table_name + ' Add ' + @AddColumnName + ' ' + @DataType + '(' + Cast(@DataTypeLength as nvarchar(100)) + ') ' + @NullVar
	End
	Else If @NumericScale is not null
	Begin
		set @sql = 'Alter Table ' + @delim_table_name + ' Add ' + @AddColumnName + ' ' + @DataType + '(18, ' + Cast(@NumericScale as nvarchar(100)) + ') ' + @NullVar
	End
	Else if @DataTypeLength is null
	Begin
		set @sql = 'Alter Table ' + @delim_table_name + ' Add ' + @AddColumnName + ' ' + @DataType + ' ' + @NullVar
	End
	--print @sql
	exec sp_executesql @sql
end
close AddColumnsLocalTable
deallocate AddColumnsLocalTable

set @ColumnList = 'Id'

--Build column list for insert statement and select statement
DECLARE ColumnsList CURSOR Local Fast_Forward FOR
		Select c.COLUMN_NAME
		from INFORMATION_SCHEMA.COLUMNS c, INFORMATION_SCHEMA.TABLES t
		Where c.TABLE_NAME = t.TABLE_NAME and t.TABLE_TYPE = 'BASE TABLE' and t.TABLE_SCHEMA = c.TABLE_SCHEMA and c.TABLE_NAME = @prev_table and c.COLUMN_NAME <> 'Id'
	OPEN ColumnsList

	While 1=1
	Begin
		FETCH NEXT FROM ColumnsList into @AddColumnName
		if @@error <> 0 or @@fetch_status <> 0 break

		set @ColumnList = @ColumnList + ', ' + '[' + @AddColumnName + ']'
		
	end
	close ColumnsList
	deallocate ColumnsList

--Build insert into, select statement and execute it
set @sql = 'Insert Into ' + @delim_table_name + '(' + @ColumnList + ')' 
			+ ' select ' + @ColumnList + ' from ' + @prev_table
exec sp_executesql @sql

GO

