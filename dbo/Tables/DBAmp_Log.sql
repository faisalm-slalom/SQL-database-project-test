CREATE TABLE [dbo].[DBAmp_Log] (
    [SPName]  [sysname]      NULL,
    [Status]  NVARCHAR (20)  NULL,
    [Message] NVARCHAR (MAX) NULL,
    [LogTime] DATETIME       DEFAULT (getdate()) NULL,
    [Seen]    INT            DEFAULT ((0)) NULL
);


GO

CREATE NONCLUSTERED INDEX [SPNameIndex]
    ON [dbo].[DBAmp_Log]([SPName] ASC);


GO

