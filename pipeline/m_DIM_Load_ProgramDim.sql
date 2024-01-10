WITH
SQ_Program AS (
	SELECT
		SourceSystemId,
		ProgramCode,
		ProgramDescription
	FROM Program
	WHERE CurrentSnapshotFlag=1
),
LKP_ProgramDim AS (
	SELECT
	ProgramDimId,
	ProgramCode
	FROM (
		SELECT 
			ProgramDimId,
			ProgramCode
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.ProgramDim
		WHERE CurrentSnapshotFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ProgramCode ORDER BY ProgramDimId) = 1
),
EXP_Value AS (
	SELECT
	LKP_ProgramDim.ProgramDimId AS lkp_ProgramDimId,
	-- *INF*: IIF(ISNULL(lkp_ProgramDimId), 'INSERT', 'UPDATE')
	IFF(lkp_ProgramDimId IS NULL, 'INSERT', 'UPDATE') AS Flag,
	1 AS CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditId,
	-- *INF*: TO_DATE('1800-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS')
	TO_DATE('1800-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS') AS EffectiveDate,
	-- *INF*: TO_DATE('2100-12-31 23:59:59','YYYY-MM-DD HH24:MI:SS')
	TO_DATE('2100-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS') AS ExpirationDate,
	SQ_Program.SourceSystemId,
	SYSDATE AS CreatedDate,
	SYSDATE AS ModifiedDate,
	SQ_Program.ProgramCode,
	SQ_Program.ProgramDescription
	FROM SQ_Program
	LEFT JOIN LKP_ProgramDim
	ON LKP_ProgramDim.ProgramCode = SQ_Program.ProgramCode
),
RTR_ProgramDim AS (
	SELECT
	lkp_ProgramDimId AS ProgramDimId,
	Flag,
	CurrentSnapshotFlag,
	AuditId,
	EffectiveDate,
	ExpirationDate,
	SourceSystemId,
	CreatedDate,
	ModifiedDate,
	ProgramCode,
	ProgramDescription
	FROM EXP_Value
),
RTR_ProgramDim_UPDATE AS (SELECT * FROM RTR_ProgramDim WHERE Flag = 'UPDATE'),
RTR_ProgramDim_DEFAULT1 AS (SELECT * FROM RTR_ProgramDim WHERE NOT ( (Flag = 'UPDATE') )),
UPD_ProgramDim_Update AS (
	SELECT
	ProgramDimId, 
	CurrentSnapshotFlag, 
	AuditId, 
	EffectiveDate, 
	ExpirationDate, 
	SourceSystemId, 
	CreatedDate, 
	ModifiedDate, 
	ProgramCode, 
	ProgramDescription
	FROM RTR_ProgramDim_UPDATE
),
TGT_ProgramDim_UPDATE AS (
	MERGE INTO ProgramDim AS T
	USING UPD_ProgramDim_Update AS S
	ON T.ProgramDimId = S.ProgramDimId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.ModifiedDate = S.ModifiedDate, T.ProgramCode = S.ProgramCode, T.ProgramDescription = S.ProgramDescription
),
UPD_ProgramDim_Insert AS (
	SELECT
	CurrentSnapshotFlag, 
	AuditId, 
	EffectiveDate, 
	ExpirationDate, 
	SourceSystemId, 
	CreatedDate, 
	ModifiedDate, 
	ProgramCode, 
	ProgramDescription
	FROM RTR_ProgramDim_DEFAULT1
),
TGT_ProgramDim_INSERT AS (
	INSERT INTO ProgramDim
	(CurrentSnapshotFlag, AuditId, EffectiveDate, ExpirationDate, SourceSystemId, CreatedDate, ModifiedDate, ProgramCode, ProgramDescription)
	SELECT 
	CURRENTSNAPSHOTFLAG, 
	AUDITID, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	SOURCESYSTEMID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	PROGRAMCODE, 
	PROGRAMDESCRIPTION
	FROM UPD_ProgramDim_Insert
),