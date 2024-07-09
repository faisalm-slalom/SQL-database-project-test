Create VIEW DBAmp_Refresh_Perf
As 
(
Select  'SF_Refresh' as Type, SPName, 

ISNULL((Select LogTime
From DBAmp_Log As a5
Where Message Like 'Ending%' and p.SPName = a5.SPName), 0) As "LogTime",

(select Data
from SF_Split(SUBSTRING(Message, 13, 1000), ' ', 1)
where Id = 1) As "LinkedServer",

(select Data
from SF_Split(SUBSTRING(Message, 13, 1000), ' ', 1)
where Id = 2) As "Object",

ISNULL((Select Top 1 Case When DATEDIFF(SS, p.LogTime, a3.LogTime) = '0' Then '1' Else DATEDIFF(SS, p.LogTime, a3.LogTime) End
From DBAmp_Log As a3
Where Message like 'Ending%' and p.SPName = a3.SPName), 0) As "RunTimeSeconds",

ISNULL((Select Case When Status = 'Failed' Then 'True' Else 'False' End
From DBAmp_Log As a4
Where Status = 'Failed' and p.SPName = a4.SPName), 'False') As "Failed",

Cast(IsNull((Select Top 1 Substring(Message, 11, Case When PATINDEX('% updated/inserted%', Message) - 11 < 0 then 0 else PATINDEX('% updated/inserted%', Message) - 11 End)
From DBAmp_Log As a1
Where PATINDEX('% updated/inserted%', Message) <> 0 and Message Like 'Identified%' and a1.SPName = p.SPName), 0) AS int)  As "RowsUpdatedOrInserted",

Cast(IsNull((Select Top 1 Substring(Message, 11, Case When PATINDEX('% deleted%', Message) - 11 < 0 then 0 else PATINDEX('% deleted%', Message) - 11 End) 
from DBAmp_Log As a2
Where PATINDEX('% deleted rows%', Message) <> 0 and Message Like 'Identified%' and a2.SPName = p.SPName), 0) AS int)  As "RowsDeleted"

From DBAmp_Log As p
Where (SPName Like 'SF_RefreshIAD:%' or SPName Like 'SF_Refresh:%' or SPName Like '%SF_RefreshTemporal:%') and Status = 'Starting' and exists (select SPName from DBAmp_Log where p.SPName = SPName and Message Like 'Ending%')
)

GO

