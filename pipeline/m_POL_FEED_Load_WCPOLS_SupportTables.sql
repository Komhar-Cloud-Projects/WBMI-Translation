WITH
SQ_SUPWCPOLS AS (

-- TODO Manual --

),
EXP_SUPWCPOLS AS (
	SELECT
	Code,
	Description,
	SourceCode,
	TableName,
	ProcessName,
	Notes,
	NCCIRequiredFlag,
	WIRequiredFlag,
	MNRequiredFlag,
	MIRequiredFlag,
	NCRequiredFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS Auditid,
	'1' AS CurrentSnapshotFlag,
	CURRENT_TIMESTAMP AS CreatedDate,
	CURRENT_TIMESTAMP AS ModifiedDate,
	'DCT' AS SourcesystemID,
	-- *INF*: Ltrim(Rtrim(Code))
	Ltrim(Rtrim(Code)) AS o_WCPOLSCode,
	-- *INF*: Ltrim(Rtrim(Description))
	Ltrim(Rtrim(Description)) AS o_WCPOLSDescription,
	-- *INF*: Ltrim(Rtrim(SourceCode))
	Ltrim(Rtrim(SourceCode)) AS o_SourceCode,
	-- *INF*: Ltrim(Rtrim(TableName))
	Ltrim(Rtrim(TableName)) AS o_TableName,
	-- *INF*: Ltrim(Rtrim(ProcessName))
	Ltrim(Rtrim(ProcessName)) AS o_ProcessName,
	-- *INF*: IIF(IN(NCCIRequiredFlag,'Y','1'),'1','0')
	IFF(NCCIRequiredFlag IN ('Y','1'), '1', '0') AS o_NCCIRequiredFlag,
	-- *INF*: IIF(IN(WIRequiredFlag,'Y','1'),'1','0')
	IFF(WIRequiredFlag IN ('Y','1'), '1', '0') AS o_WIRequiredFlag,
	-- *INF*: IIF(IN(MIRequiredFlag,'Y','1'),'1','0')
	IFF(MIRequiredFlag IN ('Y','1'), '1', '0') AS o_MIRequiredFlag,
	-- *INF*: IIF(IN(MNRequiredFlag,'Y','1'),'1','0')
	IFF(MNRequiredFlag IN ('Y','1'), '1', '0') AS o_MNRequiredFlag,
	-- *INF*: IIF(IN(NCRequiredFlag,'Y','1'),'1','0')
	IFF(NCRequiredFlag IN ('Y','1'), '1', '0') AS o_NCRequiredFlag
	FROM SQ_SUPWCPOLS
),
SupWCPOLS AS (
	TRUNCATE TABLE SupWCPOLS;
	INSERT INTO SupWCPOLS
	(Auditid, CurrentSnapshotFlag, CreatedDate, ModifiedDate, SourcesystemID, WCPOLSCode, WCPOLSDescription, SourceCode, TableName, ProcessName, NCCIRequiredFlag, WIRequiredFlag, MIRequiredFlag, MNRequiredFlag, NCRequiredFlag)
	SELECT 
	AUDITID, 
	CURRENTSNAPSHOTFLAG, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	SOURCESYSTEMID, 
	o_WCPOLSCode AS WCPOLSCODE, 
	o_WCPOLSDescription AS WCPOLSDESCRIPTION, 
	o_SourceCode AS SOURCECODE, 
	o_TableName AS TABLENAME, 
	o_ProcessName AS PROCESSNAME, 
	o_NCCIRequiredFlag AS NCCIREQUIREDFLAG, 
	o_WIRequiredFlag AS WIREQUIREDFLAG, 
	o_MIRequiredFlag AS MIREQUIREDFLAG, 
	o_MNRequiredFlag AS MNREQUIREDFLAG, 
	o_NCRequiredFlag AS NCREQUIREDFLAG
	FROM EXP_SUPWCPOLS
),
SQ_SupWCPOLSTransactionTypeNeeded AS (

-- TODO Manual --

),
EXP_SupWCPOLSTransactionTypeNeeded AS (
	SELECT
	SourceTransactionType,
	WCPOLSTransactionType,
	Notes,
	NCCIRequiredFlag,
	WIRequiredFlag,
	MNRequiredFlag,
	MIRequiredFlag,
	TableName,
	NCRequiredFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS Auditid,
	'1' AS CurrentSnapshotFlag,
	CURRENT_TIMESTAMP AS CreatedDate,
	CURRENT_TIMESTAMP AS ModifiedDate,
	'DCT' AS SourcesystemID,
	-- *INF*: Ltrim(Rtrim(SourceTransactionType))
	Ltrim(Rtrim(SourceTransactionType)) AS o_SourceTransactionType,
	-- *INF*: Ltrim(Rtrim(WCPOLSTransactionType))
	Ltrim(Rtrim(WCPOLSTransactionType)) AS o_WCPOLSTransactionType,
	-- *INF*: Ltrim(Rtrim(Notes))
	Ltrim(Rtrim(Notes)) AS o_Notes,
	-- *INF*: Ltrim(Rtrim(TableName))
	Ltrim(Rtrim(TableName)) AS o_TableName,
	-- *INF*: IIF(IN(NCCIRequiredFlag,'Y','1'),'1','0')
	IFF(NCCIRequiredFlag IN ('Y','1'), '1', '0') AS o_NCCIRequiredFlag,
	-- *INF*: IIF(IN(WIRequiredFlag,'Y','1'),'1','0')
	IFF(WIRequiredFlag IN ('Y','1'), '1', '0') AS o_WIRequiredFlag,
	-- *INF*: IIF(IN(MIRequiredFlag,'Y','1'),'1','0')
	IFF(MIRequiredFlag IN ('Y','1'), '1', '0') AS o_MIRequiredFlag,
	-- *INF*: IIF(IN(MNRequiredFlag,'Y','1'),'1','0')
	IFF(MNRequiredFlag IN ('Y','1'), '1', '0') AS o_MNRequiredFlag,
	-- *INF*: IIF(IN(NCRequiredFlag,'Y','1'),'1','0')
	IFF(NCRequiredFlag IN ('Y','1'), '1', '0') AS o_NCRequiredFlag
	FROM SQ_SupWCPOLSTransactionTypeNeeded
),
SupWCPOLSTransactionTypeNeeded AS (
	INSERT INTO SupWCPOLSTransactionTypeNeeded
	(Auditid, CurrentSnapshotFlag, CreatedDate, ModifiedDate, SourcesystemID, SourceTransactionType, WCPOLSTransactionType, Notes, TableName, NCCIRequiredFlag, WIRequiredFlag, MIRequiredFlag, MNRequiredFlag, NCRequiredFlag)
	SELECT 
	AUDITID, 
	CURRENTSNAPSHOTFLAG, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	SOURCESYSTEMID, 
	o_SourceTransactionType AS SOURCETRANSACTIONTYPE, 
	o_WCPOLSTransactionType AS WCPOLSTRANSACTIONTYPE, 
	o_Notes AS NOTES, 
	o_TableName AS TABLENAME, 
	o_NCCIRequiredFlag AS NCCIREQUIREDFLAG, 
	o_WIRequiredFlag AS WIREQUIREDFLAG, 
	o_MIRequiredFlag AS MIREQUIREDFLAG, 
	o_MNRequiredFlag AS MNREQUIREDFLAG, 
	o_NCRequiredFlag AS NCREQUIREDFLAG
	FROM EXP_SupWCPOLSTransactionTypeNeeded
),
SQ_SUPWCPOLSFieldNeeded AS (

-- TODO Manual --

),
EXP_SUPWCPOLSFieldNeeded AS (
	SELECT
	Table_name,
	Field_Name,
	NCCIRequiredFlag,
	WIRequiredFlag,
	MNRequiredFlag,
	MIRequiredFlag,
	FileDataType_A_AN_N AS FileDataType,
	NCRequiredFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS Auditid,
	'1' AS CurrentSnapshotFlag,
	CURRENT_TIMESTAMP AS CreatedDate,
	CURRENT_TIMESTAMP AS ModifiedDate,
	'DCT' AS SourcesystemID,
	-- *INF*: Ltrim(Rtrim(Table_name))
	Ltrim(Rtrim(Table_name)) AS o_TableName,
	-- *INF*: Ltrim(Rtrim(Field_Name))
	Ltrim(Rtrim(Field_Name)) AS o_FieldName,
	-- *INF*: IIF(IN(NCCIRequiredFlag,'Y','1'),'1','0')
	IFF(NCCIRequiredFlag IN ('Y','1'), '1', '0') AS o_NCCIRequiredFlag,
	-- *INF*: IIF(IN(WIRequiredFlag,'Y','1'),'1','0')
	IFF(WIRequiredFlag IN ('Y','1'), '1', '0') AS o_WIRequiredFlag,
	-- *INF*: IIF(IN(MIRequiredFlag,'Y','1'),'1','0')
	IFF(MIRequiredFlag IN ('Y','1'), '1', '0') AS o_MIRequiredFlag,
	-- *INF*: IIF(IN(MNRequiredFlag,'Y','1'),'1','0')
	IFF(MNRequiredFlag IN ('Y','1'), '1', '0') AS o_MNRequiredFlag,
	-- *INF*: Ltrim(Rtrim(FileDataType))
	Ltrim(Rtrim(FileDataType)) AS o_FieldDataType,
	-- *INF*: IIF(IN(NCRequiredFlag,'Y','1'),'1','0')
	IFF(NCRequiredFlag IN ('Y','1'), '1', '0') AS o_NCRequiredFlag
	FROM SQ_SUPWCPOLSFieldNeeded
),
SupWCPOLSFieldNeeded AS (
	TRUNCATE TABLE SupWCPOLSFieldNeeded;
	INSERT INTO SupWCPOLSFieldNeeded
	(Auditid, CurrentSnapshotFlag, CreatedDate, ModifiedDate, TableName, FieldName, NCCIRequiredFlag, WIRequiredFlag, MIRequiredFlag, MNRequiredFlag, FieldDataType, NCRequiredFlag)
	SELECT 
	AUDITID, 
	CURRENTSNAPSHOTFLAG, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	o_TableName AS TABLENAME, 
	o_FieldName AS FIELDNAME, 
	o_NCCIRequiredFlag AS NCCIREQUIREDFLAG, 
	o_WIRequiredFlag AS WIREQUIREDFLAG, 
	o_MIRequiredFlag AS MIREQUIREDFLAG, 
	o_MNRequiredFlag AS MNREQUIREDFLAG, 
	o_FieldDataType AS FIELDDATATYPE, 
	o_NCRequiredFlag AS NCREQUIREDFLAG
	FROM EXP_SUPWCPOLSFieldNeeded
),
SQ_SUPWCPOLSSmallDeductible AS (

-- TODO Manual --

),
EXP_SUPWCPOLSSmallDeductible AS (
	SELECT
	StateCode,
	FormName,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditID,
	'1' AS CurrentSnapshotFlag,
	CURRENT_TIMESTAMP AS CreatedDate,
	CURRENT_TIMESTAMP AS ModifiedDate,
	'DCT' AS SourcesystemID,
	-- *INF*: LTRIM(RTRIM(StateCode))
	LTRIM(RTRIM(StateCode)) AS o_StateCode,
	-- *INF*: LTRIM(RTRIM(FormName))
	LTRIM(RTRIM(FormName)) AS o_FormName
	FROM SQ_SUPWCPOLSSmallDeductible
),
SUPWCPOLSSmallDeductible AS (
	TRUNCATE TABLE SUPWCPOLSSmallDeductible;
	INSERT INTO SUPWCPOLSSmallDeductible
	(Auditid, CurrentSnapshotFlag, CreatedDate, ModifiedDate, SourcesystemID, StateCode, FormName)
	SELECT 
	AuditID AS AUDITID, 
	CURRENTSNAPSHOTFLAG, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	SOURCESYSTEMID, 
	o_StateCode AS STATECODE, 
	o_FormName AS FORMNAME
	FROM EXP_SUPWCPOLSSmallDeductible
),
SQ_SupClaimAdministratorFEIN AS (

-- TODO Manual --

),
EXP_SupClaimAdministratorFEIN AS (
	SELECT
	StateCode,
	AdjustingCompany,
	ClaimAdministratorFEIN,
	AffiliateTPA,
	'1' AS CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS Auditid,
	CURRENT_TIMESTAMP AS CreatedDate,
	CURRENT_TIMESTAMP AS ModifiedDate,
	-- *INF*: LTRIM(RTRIM(StateCode))
	LTRIM(RTRIM(StateCode)) AS o_StateCode,
	-- *INF*: LTRIM(RTRIM(AdjustingCompany))
	LTRIM(RTRIM(AdjustingCompany)) AS o_AdjustingCompany,
	-- *INF*: LTRIM(RTRIM(ClaimAdministratorFEIN))
	LTRIM(RTRIM(ClaimAdministratorFEIN)) AS o_ClaimAdministratorFEIN,
	-- *INF*: LTRIM(RTRIM(AffiliateTPA))
	LTRIM(RTRIM(AffiliateTPA)) AS o_AffiliateTPA
	FROM SQ_SupClaimAdministratorFEIN
),
SupClaimAdministratorFEIN AS (
	TRUNCATE TABLE SupClaimAdministratorFEIN;
	INSERT INTO SupClaimAdministratorFEIN
	(CurrentSnapshotFlag, AuditId, CreatedDate, ModifiedDate, StateCode, AdjustingCompany, ClaimAdministratorFEIN, AffiliateTPA)
	SELECT 
	CURRENTSNAPSHOTFLAG, 
	Auditid AS AUDITID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	o_StateCode AS STATECODE, 
	o_AdjustingCompany AS ADJUSTINGCOMPANY, 
	o_ClaimAdministratorFEIN AS CLAIMADMINISTRATORFEIN, 
	o_AffiliateTPA AS AFFILIATETPA
	FROM EXP_SupClaimAdministratorFEIN
),