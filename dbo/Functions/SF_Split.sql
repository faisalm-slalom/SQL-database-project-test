CREATE FUNCTION [dbo].[SF_Split]
(
      @FullText varchar(Max),
      @Delimiter varchar(5) = ',',
      @RemoveQuote bit = 0
)
RETURNS @RtnValue table
(
      Id int identity(1,1),
      Data nvarchar(max)
)
AS
BEGIN
      DECLARE @Cnt INT
      Declare @Data nvarchar(max)
      SET @Cnt = 1
 
      WHILE (CHARINDEX(@Delimiter,@FullText)>0)
      BEGIN
            set @Data = LTRIM(RTRIM(SUBSTRING(@FullText,1,CHARINDEX(@Delimiter,@FullText)-1)))
            If Left(@Data,1) ='"' AND @RemoveQuote = 1
set @Data = SUBSTRING(@Data,2,LEN(@Data)-2)
            INSERT INTO @RtnValue (Data) Values(@Data)

 
            SET @FullText = SUBSTRING(@FullText,CHARINDEX(@Delimiter,@FullText)+1,LEN(@FullText))
            SET @Cnt = @Cnt + 1
      END
 
      set @Data = LTRIM(RTRIM(@FullText))
      
      If Len(@Data) > 0
      begin
 If Left(@Data,1) ='"' AND @RemoveQuote = 1
set @Data = SUBSTRING(@Data,2,LEN(@Data)-2)
 INSERT INTO @RtnValue (Data) Values(@Data)
      end

      RETURN
END

GO

