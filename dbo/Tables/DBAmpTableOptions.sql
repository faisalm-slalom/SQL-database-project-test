CREATE TABLE [dbo].[DBAmpTableOptions] (
    [TableName] NVARCHAR (255) NOT NULL,
    [Options]   NVARCHAR (255) DEFAULT ('') NULL,
    [SkipTable] BIT            DEFAULT ((0)) NOT NULL,
    [Comments]  NVARCHAR (255) NULL,
    PRIMARY KEY CLUSTERED ([TableName] ASC)
);


GO

