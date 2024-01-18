WITH
SQ_SupWCPOLS AS (
	Select distinct SupWCPOLSFieldNeeded.TableName,
	SupWCPOLSFieldNeeded.FieldName,
	SupWCPOLSFieldNeeded.FieldDataType,
	SupWCPOLSTransactionTypeNeeded.WCPOLSTransactionType,
	SupWCPOLSTransactionTypeNeeded.SourceTransactionType,
	SupWCPOLS.WCPOLSCode,
	case when SupWCPOLSTransactionTypeNeeded.FileName='NCCI' and SupWCPOLSFieldNeeded.FileName='NCCI' and (SupWCPOLS.FileName='NCCI' or SupWCPOLS.FileName is null) then 'NCCI' 
	 when SupWCPOLSTransactionTypeNeeded.FileName='WI' and SupWCPOLSFieldNeeded.FileName='WI' and (SupWCPOLS.FileName='WI' or SupWCPOLS.FileName is null) then 'WI'
	 when SupWCPOLSTransactionTypeNeeded.FileName='MI' and SupWCPOLSFieldNeeded.FileName='MI' and (SupWCPOLS.FileName='MI' or SupWCPOLS.FileName is null) then 'MI'
	 when SupWCPOLSTransactionTypeNeeded.FileName='MN' and SupWCPOLSFieldNeeded.FileName='MN' and (SupWCPOLS.FileName='MN' or SupWCPOLS.FileName is null) then 'MN'
	 when SupWCPOLSTransactionTypeNeeded.FileName='NC' and SupWCPOLSFieldNeeded.FileName='NC' and (SupWCPOLS.FileName='NC' or SupWCPOLS.FileName is null) then 'NC'
	END FinalFileName
	 from (
	select TableName,case when supWCPOLSFieldNeeded.FieldName like 'StateCode%' and supWCPOLSFieldNeeded.FieldName<>'StateCodeLink' then 'StateCode' else supWCPOLSFieldNeeded.FieldName end FieldName,FieldDataType,case when NCCIRequiredFlag=1 then 'NCCI' else '' end FileName from @{pipeline().parameters.SOURCE_TABLE_OWNER}.SupWCPOLSFieldNeeded
	where CurrentSnapshotFlag=1
	UNION ALL
	select TableName,case when supWCPOLSFieldNeeded.FieldName like 'StateCode%' and supWCPOLSFieldNeeded.FieldName<>'StateCodeLink' then 'StateCode' else supWCPOLSFieldNeeded.FieldName end FieldName,FieldDataType,case when WIRequiredFlag=1 then 'WI' else '' end FileName from @{pipeline().parameters.SOURCE_TABLE_OWNER}.SupWCPOLSFieldNeeded
	where CurrentSnapshotFlag=1
	UNION ALL
	select TableName,case when supWCPOLSFieldNeeded.FieldName like 'StateCode%' and supWCPOLSFieldNeeded.FieldName<>'StateCodeLink' then 'StateCode' else supWCPOLSFieldNeeded.FieldName end FieldName,FieldDataType,case when MIRequiredFlag=1 then 'MI' else '' end FileName from @{pipeline().parameters.SOURCE_TABLE_OWNER}.SupWCPOLSFieldNeeded
	where CurrentSnapshotFlag=1
	UNION ALL
	select TableName,case when supWCPOLSFieldNeeded.FieldName like 'StateCode%' and supWCPOLSFieldNeeded.FieldName<>'StateCodeLink' then 'StateCode' else supWCPOLSFieldNeeded.FieldName end FieldName,FieldDataType,case when MNRequiredFlag=1 then 'MN' else '' end FileName from @{pipeline().parameters.SOURCE_TABLE_OWNER}.SupWCPOLSFieldNeeded
	where CurrentSnapshotFlag=1
	UNION ALL
	select TableName,case when supWCPOLSFieldNeeded.FieldName like 'StateCode%' and supWCPOLSFieldNeeded.FieldName<>'StateCodeLink' then 'StateCode' else supWCPOLSFieldNeeded.FieldName end FieldName,FieldDataType,case when NCRequiredFlag=1 then 'NC' else '' end FileName from @{pipeline().parameters.SOURCE_TABLE_OWNER}.SupWCPOLSFieldNeeded
	where CurrentSnapshotFlag=1) SupWCPOLSFieldNeeded
	
	
	Left Join 
	
	(select TableName,WCPOLSTransactionType,SourceTransactionType,case when NCCIRequiredFlag=1 then 'NCCI' else '' end FileName from @{pipeline().parameters.SOURCE_TABLE_OWNER}.SupWCPOLSTransactionTypeNeeded
	where CurrentSnapshotFlag=1
	UNION
	select TableName,WCPOLSTransactionType,SourceTransactionType,case when WIRequiredFlag=1 then 'WI' else '' end FileName from @{pipeline().parameters.SOURCE_TABLE_OWNER}.SupWCPOLSTransactionTypeNeeded
	where CurrentSnapshotFlag=1
	UNION
	select TableName,WCPOLSTransactionType,SourceTransactionType,case when MIRequiredFlag=1 then 'MI' else '' end FileName from @{pipeline().parameters.SOURCE_TABLE_OWNER}.SupWCPOLSTransactionTypeNeeded
	where CurrentSnapshotFlag=1
	UNION
	select TableName,WCPOLSTransactionType,SourceTransactionType,case when MNRequiredFlag=1 then 'MN' else '' end FileName from @{pipeline().parameters.SOURCE_TABLE_OWNER}.SupWCPOLSTransactionTypeNeeded
	where CurrentSnapshotFlag=1
	UNION
	select TableName,WCPOLSTransactionType,SourceTransactionType,case when NCRequiredFlag=1 then 'NC' else '' end FileName from @{pipeline().parameters.SOURCE_TABLE_OWNER}.SupWCPOLSTransactionTypeNeeded
	where CurrentSnapshotFlag=1) SupWCPOLSTransactionTypeNeeded
	on SupWCPOLSFieldNeeded.TableName=SupWCPOLSTransactionTypeNeeded.TableName
	
	Left Join 
	
	(select TableName,case when SupWCPOLS.ProcessName like 'StateCode%' then 'StateCode' else SupWCPOLS.ProcessName end FieldName,WCPOLSCode,case when NCCIRequiredFlag=1 then 'NCCI' else '' end FileName from @{pipeline().parameters.SOURCE_TABLE_OWNER}.SupWCPOLS
	where CurrentSnapshotFlag=1
	UNION
	select TableName,case when SupWCPOLS.ProcessName like 'StateCode%' then 'StateCode' else SupWCPOLS.ProcessName end FieldName,WCPOLSCode,case when WIRequiredFlag=1 then 'WI' else '' end FileName from @{pipeline().parameters.SOURCE_TABLE_OWNER}.SupWCPOLS
	where CurrentSnapshotFlag=1
	UNION
	select TableName,case when SupWCPOLS.ProcessName like 'StateCode%' then 'StateCode' else SupWCPOLS.ProcessName end FieldName,WCPOLSCode,case when MIRequiredFlag=1 then 'MI' else '' end FileName from @{pipeline().parameters.SOURCE_TABLE_OWNER}.SupWCPOLS
	where CurrentSnapshotFlag=1
	UNION
	select TableName,case when SupWCPOLS.ProcessName like 'StateCode%' then 'StateCode' else SupWCPOLS.ProcessName end FieldName,WCPOLSCode,case when MNRequiredFlag=1 then 'MN' else '' end FileName from @{pipeline().parameters.SOURCE_TABLE_OWNER}.SupWCPOLS
	where CurrentSnapshotFlag=1
	UNION
	select TableName,case when SupWCPOLS.ProcessName like 'StateCode%' then 'StateCode' else SupWCPOLS.ProcessName end FieldName,WCPOLSCode,case when NCRequiredFlag=1 then 'NC' else '' end FileName from @{pipeline().parameters.SOURCE_TABLE_OWNER}.SupWCPOLS
	where CurrentSnapshotFlag=1) SupWCPOLS
	on SupWCPOLSFieldNeeded.TableName=SupWCPOLS.TableName
	and supWCPOLSFieldNeeded.FieldName=SupWCPOLS.FieldName
),
EXP_DataCollect AS (
	SELECT
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS Auditid,
	CURRENT_TIMESTAMP AS ExtractDate,
	TableName,
	FieldName,
	FieldDataType,
	WCPOLSTransactionType,
	SourceTransactionType,
	WCPOLSCode,
	FinalFileName
	FROM SQ_SupWCPOLS
),
FIL_EmptyFile AS (
	SELECT
	Auditid, 
	ExtractDate, 
	TableName, 
	FieldName, 
	FieldDataType, 
	WCPOLSTransactionType, 
	SourceTransactionType, 
	WCPOLSCode, 
	FinalFileName
	FROM EXP_DataCollect
	WHERE NOT ISNULL(FinalFileName)
),
SUPWCPOLSAllCombinations AS (
	TRUNCATE TABLE SUPWCPOLSAllCombinations;
	INSERT INTO SUPWCPOLSAllCombinations
	(Auditid, ExtractDate, TableName, FieldName, FieldDataType, WCPOLSTransactionType, SourceTransactionType, WCPOLSCode, FinalFileName)
	SELECT 
	AUDITID, 
	EXTRACTDATE, 
	TABLENAME, 
	FIELDNAME, 
	FIELDDATATYPE, 
	WCPOLSTRANSACTIONTYPE, 
	SOURCETRANSACTIONTYPE, 
	WCPOLSCODE, 
	FINALFILENAME
	FROM FIL_EmptyFile
),