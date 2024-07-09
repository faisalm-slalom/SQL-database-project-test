Create VIEW DBAmp_BulkOps_Perf
As 
(
Select 'SF_BulkOps' as Type, SPName, 

ISNULL((Select LogTime
From DBAmp_Log As i6
Where Message Like 'Ending%' and o.SPName = i6.SPName), 0) As "LogTime",

(select Data
from SF_Split(SUBSTRING(Message, 13, 1000), ' ', 1)
where Id = 2) As "LinkedServer",

(select Data
from SF_Split(SUBSTRING(Message, 13, 1000), ' ', 1)
where Id = 3) As "Object",

(Select Top 1 Case When DATEDIFF(SS, o.LogTime, i4.LogTime) = '0' Then '1' Else DATEDIFF(SS, o.LogTime, i4.LogTime) End
From DBAmp_Log As i4
Where Message Like '%Ending%' and o.SPName = i4.SPName) As "RunTimeSeconds",

ISNULL((Select Case When Status = 'Failed' Then 'True' Else 'False' End
From DBAmp_Log As i5
Where Status = 'Failed' and o.SPName = i5.SPName), 'False') As "Failed",

(select Data
from SF_Split(SUBSTRING(Message, 13, 1000), ' ', 1)
where Id = 1) As "BulkOpsAction",

Cast(ISNULL((Select Top 1 Substring(Message, 11, Case When PATINDEX('% rows read%', Message) - 11 < 0 then 0 else PATINDEX('% rows read%', Message) - 11 End) 
From DBAmp_Log As i1
Where PATINDEX('% rows read%', Message) <> 0 and i1.SPName = o.SPName ), 0) as int) As "RowsRead",

Cast(ISNULL((Select Top 1 Substring(Message, 11, Case When PATINDEX('% rows successfully%', Message) - 11 < 0 then 0 else PATINDEX('% rows successfully%', Message) - 11 End) 
From DBAmp_Log As i2
Where PATINDEX('% rows successfully%', Message) <> 0 and i2.SPName = o.SPName), 0) As Int) As "RowsSuccessfull",

Cast(ISNULL((Select Top 1 Substring(Message, 11, Case When PATINDEX('% rows fail%', Message) - 11 < 0 then 0 else PATINDEX('% rows fail%', Message) - 11 End)  
From DBAmp_Log As i3
Where PATINDEX('% rows fail%', Message) <> 0 and i3.SPName = o.SPName), 0) As Int) + 
Cast(ISNULL((Select Top 1 Substring(Message, 11, Case When PATINDEX('% rows unprocessed%', Message) - 11 < 0 then 0 else PATINDEX('% rows unprocessed%', Message) - 11 End)  
From DBAmp_Log As i3
Where PATINDEX('% rows unprocessed%', Message) <> 0 and i3.SPName = o.SPName), 0) As Int) As "RowsFailed"

From DBAmp_Log As o
Where SPName Like '%SF_Bulk%' and Status = 'Starting' and exists (select SPName from DBAmp_Log where o.SPName = SPName and Message Like 'Ending%')
)

GO

