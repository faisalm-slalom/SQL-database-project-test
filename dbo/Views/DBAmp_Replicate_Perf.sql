Create VIEW DBAmp_Replicate_Perf
As 
(
Select 'SF_Replicate' as Type, SPName,

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

Cast(ISNULL((Select Substring(Message, 11, Case When PATINDEX('% rows copied.%', Message) - 11 < 0 then 0 else PATINDEX('% rows copied.%', Message) - 11 End)
From DBAmp_Log As b1
Where PATINDEX('% rows copied.%', Message) <> 0 and b1.SPName = r.SPName), 0) AS Int) As "RowsCopied"

From DBAmp_Log As r
Where (SPName Like '%SF_Replicate:%' 
or SPName Like '%SF_ReplicateIAD:%'
or SPName Like '%SF_ReplicateLarge:%'
or SPName Like '%SF_ReplicateHistory:%'   
or SPName Like '%SF_ReplicateKAV:%' 
or SPName Like '%SF_Replicate3:%'
or SPName Like '%SF_ReplicateTemporal:%')
 and Status = 'Starting' 
 and exists (select SPName from DBAmp_Log where r.SPName = SPName and Message Like 'Ending%')
)

GO

