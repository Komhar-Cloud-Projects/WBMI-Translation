WITH
SQ_CLAIM_SUPPORT_01_STAGE AS (
	SELECT RTRIM(CS.CS01_CODE) AS CS01_CODE, RTRIM(CS.CS01_CODE_DES) AS CS01_CODE_DES, CS.SOURCE_SYSTEM_ID 
	FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.CLAIM_SUPPORT_01_STAGE CS
	WHERE CS.CS01_TABLE_ID = 'W033'
),
EXP_Source AS (
	SELECT
	CS01_CODE,
	CS01_CODE_DES,
	SOURCE_SYSTEM_ID
	FROM SQ_CLAIM_SUPPORT_01_STAGE
),
LKP_SupClaimReportedMethodDescription AS (
	SELECT
	SupClaimReportedMethodDescriptionId,
	ClaimReportedMethodCode,
	ClaimReportedMethodDescription
	FROM (
		select SupClaimReportedMethodDescriptionId as SupClaimReportedMethodDescriptionId, 
			ClaimReportedMethodCode as ClaimReportedMethodCode, 
			ClaimReportedMethodDescription as ClaimReportedMethodDescription
		from RPT_EDM.dbo.SupClaimReportedMethodDescription
		where CurrentSnapshotFlag = 1 
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ClaimReportedMethodCode ORDER BY SupClaimReportedMethodDescriptionId) = 1
),
EXP_Detect_Changes AS (
	SELECT
	EXP_Source.CS01_CODE,
	EXP_Source.CS01_CODE_DES,
	EXP_Source.SOURCE_SYSTEM_ID,
	LKP_SupClaimReportedMethodDescription.SupClaimReportedMethodDescriptionId AS lkp_SupClaimReportedMethodDescriptionId,
	LKP_SupClaimReportedMethodDescription.ClaimReportedMethodCode AS lkp_ClaimReportedMethodCode,
	LKP_SupClaimReportedMethodDescription.ClaimReportedMethodDescription AS lkp_ClaimReportedMethodDescription,
	-- *INF*: iif(isnull(lkp_SupClaimReportedMethodDescriptionId),
	--     'NEW',
	--     iif(LTRIM(RTRIM(CS01_CODE)) != LTRIM(RTRIM(lkp_ClaimReportedMethodCode)) 
	--         OR
	--     LTRIM(RTRIM(CS01_CODE_DES)) != LTRIM(RTRIM(lkp_ClaimReportedMethodDescription)),
	--         'UPDATE', 
	-- 'NOCHANGE'))
	IFF(lkp_SupClaimReportedMethodDescriptionId IS NULL,
		'NEW',
		IFF(LTRIM(RTRIM(CS01_CODE
				)
			) != LTRIM(RTRIM(lkp_ClaimReportedMethodCode
				)
			) 
			OR LTRIM(RTRIM(CS01_CODE_DES
				)
			) != LTRIM(RTRIM(lkp_ClaimReportedMethodDescription
				)
			),
			'UPDATE',
			'NOCHANGE'
		)
	) AS v_ChangedFlag,
	v_ChangedFlag AS ChangeFlag,
	-- *INF*: iif(v_ChangedFlag='NEW',
	-- 	to_date('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'),
	--     sysdate)
	IFF(v_ChangedFlag = 'NEW',
		to_date('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'
		),
		sysdate
	) AS EffectiveDate,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS'
	) AS ExpirationDate,
	SYSDATE AS CurrentDate,
	1 AS CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditId
	FROM EXP_Source
	LEFT JOIN LKP_SupClaimReportedMethodDescription
	ON LKP_SupClaimReportedMethodDescription.ClaimReportedMethodCode = EXP_Source.CS01_CODE
),
FIL_NewOrChanged AS (
	SELECT
	CS01_CODE, 
	CS01_CODE_DES, 
	SOURCE_SYSTEM_ID, 
	ChangeFlag, 
	EffectiveDate, 
	ExpirationDate, 
	CurrentDate, 
	CurrentSnapshotFlag, 
	AuditId
	FROM EXP_Detect_Changes
	WHERE ChangeFlag = 'NEW' or ChangeFlag = 'UPDATE'
),
SupClaimReportedMethodDescription_Insert AS (
	INSERT INTO SupClaimReportedMethodDescription
	(CurrentSnapshotFlag, AuditId, EffectiveDate, ExpirationDate, SourceSystemId, CreatedDate, ModifiedDate, ClaimReportedMethodCode, ClaimReportedMethodDescription)
	SELECT 
	CURRENTSNAPSHOTFLAG, 
	AUDITID, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	SOURCE_SYSTEM_ID AS SOURCESYSTEMID, 
	CurrentDate AS CREATEDDATE, 
	CurrentDate AS MODIFIEDDATE, 
	CS01_CODE AS CLAIMREPORTEDMETHODCODE, 
	CS01_CODE_DES AS CLAIMREPORTEDMETHODDESCRIPTION
	FROM FIL_NewOrChanged
),
SQ_SupClaimReportedMethodDescription_Type2 AS (
	SELECT a.SupClaimReportedMethodDescriptionId,
		a.ClaimReportedMethodCode,
		a.EffectiveDate,
		a.ExpirationDate
	FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.SupClaimReportedMethodDescription a
	WHERE EXISTS (
			SELECT 1
			FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.SupClaimReportedMethodDescription b
			WHERE CurrentSnapshotFlag = 1
				AND a.ClaimReportedMethodCode = b.ClaimReportedMethodCode
			GROUP BY b.ClaimReportedMethodCode
			HAVING COUNT(1) > 1
			)
	ORDER BY a.ClaimReportedMethodCode,
		a.EffectiveDate DESC
),
EXPTRANS AS (
	SELECT
	SupClaimReportedMethodDescriptionId,
	ClaimReportedMethodCode,
	EffectiveDate,
	ExpirationDate AS orig_ExpirationDate,
	-- *INF*: DECODE(TRUE,
	-- 	ClaimReportedMethodCode = v_PREV_ROW_ClaimReportedMethodCode, 
	-- 		ADD_TO_DATE(v_PREV_ROW_EffectiveDate,'SS',-1), 
	-- 	orig_ExpirationDate)
	DECODE(TRUE,
		ClaimReportedMethodCode = v_PREV_ROW_ClaimReportedMethodCode, DATEADD(SECOND,- 1,v_PREV_ROW_EffectiveDate),
		orig_ExpirationDate
	) AS v_ExpirationDate,
	v_ExpirationDate AS ExpirationDate,
	ClaimReportedMethodCode AS v_PREV_ROW_ClaimReportedMethodCode,
	EffectiveDate AS v_PREV_ROW_EffectiveDate,
	0 AS CurrentSnapshotFlag,
	SYSDATE AS CurrentDate
	FROM SQ_SupClaimReportedMethodDescription_Type2
),
FIL_First_Row_In_AK_Group AS (
	SELECT
	SupClaimReportedMethodDescriptionId, 
	orig_ExpirationDate, 
	ExpirationDate, 
	CurrentSnapshotFlag, 
	CurrentDate
	FROM EXPTRANS
	WHERE orig_ExpirationDate != ExpirationDate
),
UPD_EffectiveDate AS (
	SELECT
	SupClaimReportedMethodDescriptionId, 
	ExpirationDate, 
	CurrentSnapshotFlag, 
	CurrentDate
	FROM FIL_First_Row_In_AK_Group
),
SupClaimReportedMethodDescription_Update AS (
	MERGE INTO SupClaimReportedMethodDescription AS T
	USING UPD_EffectiveDate AS S
	ON T.SupClaimReportedMethodDescriptionId = S.SupClaimReportedMethodDescriptionId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.CurrentSnapshotFlag = S.CurrentSnapshotFlag, T.ExpirationDate = S.ExpirationDate, T.ModifiedDate = S.CurrentDate
),