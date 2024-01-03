WITH
SQ_sup_reason_amended_code AS (
	SELECT
		sup_rsn_amended_code_id,
		crrnt_snpsht_flag,
		audit_id,
		eff_from_date,
		eff_to_date,
		source_sys_id,
		created_date,
		modified_date,
		rsn_amended_code,
		rsn_amended_code_descript,
		StandardReasonAmendedCode,
		StandardReasonAmendedDescription
	FROM sup_reason_amended_code
),
AGG_reason_amended_code AS (
	SELECT
	StandardReasonAmendedCode, 
	StandardReasonAmendedDescription
	FROM SQ_sup_reason_amended_code
	QUALIFY ROW_NUMBER() OVER (PARTITION BY StandardReasonAmendedCode ORDER BY NULL) = 1
),
LKP_ReasonAmendedCodeDim AS (
	SELECT
	ReasonAmendedCodeDimId,
	in_StandardReasonAmendedCode,
	ReasonAmendedCode
	FROM (
		SELECT 
			ReasonAmendedCodeDimId,
			in_StandardReasonAmendedCode,
			ReasonAmendedCode
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.ReasonAmendedCodeDim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ReasonAmendedCode ORDER BY ReasonAmendedCodeDimId) = 1
),
EXP_ExistingChecking AS (
	SELECT
	LKP_ReasonAmendedCodeDim.ReasonAmendedCodeDimId AS lkp_ReasonAmendedCodeDimId,
	-- *INF*: IIF(ISNULL(lkp_ReasonAmendedCodeDimId), 'Insert', 
	--  'Update')
	IFF(lkp_ReasonAmendedCodeDimId IS NULL, 'Insert', 'Update') AS v_ChangeFlag,
	v_ChangeFlag AS ChangeFlag,
	1 AS CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditId,
	-- *INF*: TO_DATE('1800-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS')
	TO_DATE('1800-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS') AS EffectiveDate,
	-- *INF*: TO_DATE('2100-12-31 23:59:59','YYYY-MM-DD HH24:MI:SS')
	TO_DATE('2100-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS') AS ExpirationDate,
	SYSDATE AS CreatedDate,
	SYSDATE AS ModifiedDate,
	AGG_reason_amended_code.StandardReasonAmendedCode AS i_StandardReasonAmendedCode,
	AGG_reason_amended_code.StandardReasonAmendedDescription AS i_StandardReasonAmendedDescription,
	-- *INF*: IIF(ISNULL(i_StandardReasonAmendedCode ), 'N/A', i_StandardReasonAmendedCode )
	IFF(i_StandardReasonAmendedCode IS NULL, 'N/A', i_StandardReasonAmendedCode) AS o_StandardReasonAmendedCode,
	-- *INF*: IIF(ISNULL(i_StandardReasonAmendedDescription ), 'N/A', i_StandardReasonAmendedDescription )
	IFF(i_StandardReasonAmendedDescription IS NULL, 'N/A', i_StandardReasonAmendedDescription) AS o_StandardReasonAmendedDescription
	FROM AGG_reason_amended_code
	LEFT JOIN LKP_ReasonAmendedCodeDim
	ON LKP_ReasonAmendedCodeDim.ReasonAmendedCode = AGG_reason_amended_code.StandardReasonAmendedCode
),
RTR_InsertUpdate AS (
	SELECT
	lkp_ReasonAmendedCodeDimId AS ReasonAmendedCodeDimID,
	ChangeFlag,
	CurrentSnapshotFlag,
	AuditId,
	CreatedDate,
	ModifiedDate,
	o_StandardReasonAmendedCode AS ReasonAmendedCode,
	o_StandardReasonAmendedDescription AS ReasonAmendedDescription
	FROM EXP_ExistingChecking
),
RTR_InsertUpdate_Insert AS (SELECT * FROM RTR_InsertUpdate WHERE ChangeFlag = 'Insert'),
RTR_InsertUpdate_DEFAULT1 AS (SELECT * FROM RTR_InsertUpdate WHERE NOT ( (ChangeFlag = 'Insert') )),
UPD_QuoteStatusDim_Insert AS (
	SELECT
	AuditId AS AuditID, 
	CreatedDate, 
	ModifiedDate, 
	ReasonAmendedCode, 
	ReasonAmendedDescription
	FROM RTR_InsertUpdate_Insert
),
ReasonAmendedCodeDim_Insert AS (
	INSERT INTO ReasonAmendedCodeDim
	(AuditID, CreatedDate, ModifiedDate, ReasonAmendedCode, ReasonAmendedDescription)
	SELECT 
	AUDITID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	REASONAMENDEDCODE, 
	REASONAMENDEDDESCRIPTION
	FROM UPD_QuoteStatusDim_Insert
),
UPD_QuoteStatusDim_Update AS (
	SELECT
	ReasonAmendedCodeDimID AS ReasonAmendedCodeDimId, 
	AuditId AS AuditID, 
	CreatedDate, 
	ModifiedDate, 
	ReasonAmendedCode, 
	ReasonAmendedDescription
	FROM RTR_InsertUpdate_DEFAULT1
),
ReasonAmendedCodeDim_update AS (
	MERGE INTO ReasonAmendedCodeDim AS T
	USING UPD_QuoteStatusDim_Update AS S
	ON T.ReasonAmendedCodeDimId = S.ReasonAmendedCodeDimId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.AuditID = S.AuditID, T.ModifiedDate = S.ModifiedDate, T.ReasonAmendedCode = S.ReasonAmendedCode, T.ReasonAmendedDescription = S.ReasonAmendedDescription
),