Create VIEW DBAmp_Mirror_Perf
As 
(
Select 'SF_Mirror' as Type, SPName,

ISNULL((Select LogTime
From DBAmp_Log As b4
Where Message Like 'Ending%' and r.SPName = b4.SPName), 0) As "LogTime",

(select Data
from SF_Split(SUBSTRING(Message, 13, 1000), ' ', 1)
where Id = 1) As "LinkedServer",

(select Data
from SF_Split(SUBSTRING(Message, 13, 1000), ' ', 1)
where Id = 2) As "Object",

ISNULL((Select Top 1 Case When DATEDIFF(SS, r.LogTime, b2.LogTime) = '0' Then '1' Else DATEDIFF(SS, r.LogTime, b2.LogTime) End
From DBAmp_Log As b2
Where Message like '%Ending%' and r.SPName = b2.SPName), 0) As "RunTimeSeconds",

ISNULL((Select Case When Status = 'Failed' Then 'True' Else 'False' End
From DBAmp_Log As b3
Where Status = 'Failed' and r.SPName = b3.SPName), 'False') As "Failed",

Cast(ISNULL((Select Substring(Message, 10, Case When PATINDEX('% rows copied.%', Message) - 10 < 0 then 0 else PATINDEX('% rows copied.%', Message) - 10 End)
From DBAmp_Log As b1
Where PATINDEX('% rows copied.%', Message) <> 0 and b1.SPName = r.SPName), 0) AS Int) As "RowsCopied",

Cast(IsNull((Select Top 1 Substring(Message, 21, Case When PATINDEX('% updated / inserted%', Message) - 21 < 0 then 0 else PATINDEX('% updated / inserted%', Message) - 21 End)
From DBAmp_Log As b4
Where PATINDEX('% updated / inserted%', Message) <> 0 and Message Like '%Identified%' and b4.SPName = r.SPName), 0) AS int) As "RowsUpdatedOrInserted",

Cast(IsNull((Select Top 1 Substring(Message, 21, Case When PATINDEX('% deleted%', Message) - 21 < 0 then 0 else PATINDEX('% deleted%', Message) - 21 End) 
from DBAmp_Log As b5
Where PATINDEX('% deleted rows%', Message) <> 0 and Message Like '%Identified%' and b5.SPName = r.SPName), 0) AS int) As "RowsDeleted"

From DBAmp_Log As r
Where SPName Like '%SF_Mirror:%' 
 and Status = 'Starting' 
 and exists (select SPName from DBAmp_Log where r.SPName = SPName and Message Like 'Ending%')
)

GO

