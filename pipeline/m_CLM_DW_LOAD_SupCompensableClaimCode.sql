WITH
SQ_CLAIM_SUPPORT_01_STAGE AS (
	SELECT RTRIM(CS.CS01_CODE) AS CS01_CODE, RTRIM(CS.CS01_CODE_DES) AS CS01_CODE_DES, CS.SOURCE_SYSTEM_ID 
	FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.CLAIM_SUPPORT_01_STAGE CS
	WHERE CS.CS01_TABLE_ID = 'W034'
),
EXP_Source AS (
	SELECT
	CS01_CODE,
	CS01_CODE_DES,
	SOURCE_SYSTEM_ID
	FROM SQ_CLAIM_SUPPORT_01_STAGE
),
LKP_SupCompensableClaimCode AS (
	SELECT
	CompensableClaimDescription,
	SupCompensableClaimCode
	FROM (
		SELECT 
			CompensableClaimDescription,
			SupCompensableClaimCode
		FROM SupCompensableClaimCode
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY SupCompensableClaimCode ORDER BY CompensableClaimDescription) = 1
),
EXP_Detect_Changes AS (
	SELECT
	EXP_Source.CS01_CODE,
	EXP_Source.CS01_CODE_DES,
	EXP_Source.SOURCE_SYSTEM_ID,
	LKP_SupCompensableClaimCode.CompensableClaimDescription AS lkp_CompensableClaimDescription,
	-- *INF*: iif(isnull(lkp_CompensableClaimDescription),
	--     'NEW',
	--     iif(LTRIM(RTRIM(CS01_CODE_DES)) != LTRIM(RTRIM(lkp_CompensableClaimDescription)),
	--         'UPDATE', 
	-- 'NOCHANGE'))
	IFF(
	    lkp_CompensableClaimDescription IS NULL, 'NEW',
	    IFF(
	        LTRIM(RTRIM(CS01_CODE_DES)) != LTRIM(RTRIM(lkp_CompensableClaimDescription)),
	        'UPDATE',
	        'NOCHANGE'
	    )
	) AS v_ChangeFlag,
	v_ChangeFlag AS ChangeFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditId,
	SYSDATE AS CurrentDate
	FROM EXP_Source
	LEFT JOIN LKP_SupCompensableClaimCode
	ON LKP_SupCompensableClaimCode.SupCompensableClaimCode = EXP_Source.CS01_CODE
),
RTR_InsertOrUpdate AS (
	SELECT
	CS01_CODE,
	CS01_CODE_DES,
	SOURCE_SYSTEM_ID,
	ChangeFlag,
	AuditId,
	CurrentDate
	FROM EXP_Detect_Changes
),
RTR_InsertOrUpdate_INSERT AS (SELECT * FROM RTR_InsertOrUpdate WHERE ChangeFlag='NEW'),
RTR_InsertOrUpdate_UPDATE AS (SELECT * FROM RTR_InsertOrUpdate WHERE ChangeFlag='UPDATE'),
SupCompensableClaimCode_Insert AS (
	INSERT INTO SupCompensableClaimCode
	(SupCompensableClaimCode, AuditId, SourceSystemId, CreatedDate, ModifiedDate, CompensableClaimDescription)
	SELECT 
	CS01_CODE AS SUPCOMPENSABLECLAIMCODE, 
	AUDITID, 
	SOURCE_SYSTEM_ID AS SOURCESYSTEMID, 
	CurrentDate AS CREATEDDATE, 
	CurrentDate AS MODIFIEDDATE, 
	CS01_CODE_DES AS COMPENSABLECLAIMDESCRIPTION
	FROM RTR_InsertOrUpdate_INSERT
),
UPDTRANS AS (
	SELECT
	CS01_CODE AS CS01_CODE3, 
	CS01_CODE_DES AS CS01_CODE_DES3, 
	CurrentDate AS CurrentDate3
	FROM RTR_InsertOrUpdate_UPDATE
),
SupCompensableClaimCode_Update AS (
	MERGE INTO SupCompensableClaimCode AS T
	USING UPDTRANS AS S
	ON T.SupCompensableClaimCode = S.CS01_CODE3
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.ModifiedDate = S.CurrentDate3, T.CompensableClaimDescription = S.CS01_CODE_DES3
),