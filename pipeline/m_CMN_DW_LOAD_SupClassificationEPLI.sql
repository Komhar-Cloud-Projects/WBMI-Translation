WITH
LKP_SupClassificationEPLI_CurrentChangeFlag AS (
	SELECT
	SupClassificationEPLIId,
	RatingStateCode,
	ClassCode,
	EffectiveDate,
	ClassDescription,
	OriginatingOrganizationCode
	FROM (
		SELECT 
			SupClassificationEPLIId,
			RatingStateCode,
			ClassCode,
			EffectiveDate,
			ClassDescription,
			OriginatingOrganizationCode
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.SupClassificationEPLI
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY RatingStateCode,ClassCode,EffectiveDate,ClassDescription,OriginatingOrganizationCode ORDER BY SupClassificationEPLIId) = 1
),
SQ_SupClassificationEPLI AS (
	SELECT
		SupClassificationEPLIId,
		AuditId,
		CreatedDate,
		ModifiedDate,
		LineOfBusinessAbbreviation,
		RatingStateCode,
		EffectiveDate,
		ExpirationDate,
		ClassCode,
		ClassDescription,
		OriginatingOrganizationCode
	FROM SupClassificationEPLI
),
LKP_SupClassificationEPLI AS (
	SELECT
	SupClassificationEPLIId,
	EffectiveDate,
	ExpirationDate,
	RatingStateCode,
	ClassCode,
	ClassDescription,
	OriginatingOrganizationCode
	FROM (
		SELECT 
			SupClassificationEPLIId,
			EffectiveDate,
			ExpirationDate,
			RatingStateCode,
			ClassCode,
			ClassDescription,
			OriginatingOrganizationCode
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.SupClassificationEPLI
		WHERE CurrentSnapshotFlag =1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY RatingStateCode,OriginatingOrganizationCode,ClassCode ORDER BY SupClassificationEPLIId) = 1
),
EXP_Detect_Changes AS (
	SELECT
	SQ_SupClassificationEPLI.EffectiveDate AS i_EffectiveDate,
	SQ_SupClassificationEPLI.ExpirationDate AS i_ExpirationDate,
	SQ_SupClassificationEPLI.LineOfBusinessAbbreviation AS i_LineOfBusinessAbbreviation,
	SQ_SupClassificationEPLI.RatingStateCode AS i_RatingStateCode,
	SQ_SupClassificationEPLI.ClassCode AS i_ClassCode,
	SQ_SupClassificationEPLI.ClassDescription AS i_ClassDescription,
	SQ_SupClassificationEPLI.OriginatingOrganizationCode AS i_OriginatingOrganizationCode,
	LKP_SupClassificationEPLI.SupClassificationEPLIId AS lkp_SupClassificationEPLIId,
	LKP_SupClassificationEPLI.EffectiveDate AS lkp_EffectiveDate,
	LKP_SupClassificationEPLI.ExpirationDate AS lkp_ExpirationDate,
	LKP_SupClassificationEPLI.RatingStateCode AS lkp_RatingStateCode,
	LKP_SupClassificationEPLI.ClassCode AS lkp_ClassCode,
	LKP_SupClassificationEPLI.ClassDescription AS lkp_ClassDescription,
	LKP_SupClassificationEPLI.OriginatingOrganizationCode AS lkp_OriginatingOrganizationCode,
	-- *INF*: DECODE(TRUE,
	-- NOT ISNULL(:LKP.LKP_SUPCLASSIFICATIONEPLI_CURRENTCHANGEFLAG(i_RatingStateCode,i_ClassCode,i_EffectiveDate,i_ClassDescription,i_OriginatingOrganizationCode)),
	-- 'NOCHANGE',
	-- 'INSERT')						
	DECODE(TRUE,
		LKP_SUPCLASSIFICATIONEPLI_CURRENTCHANGEFLAG_i_RatingStateCode_i_ClassCode_i_EffectiveDate_i_ClassDescription_i_OriginatingOrganizationCode.SupClassificationEPLIId IS NOT NULL, 'NOCHANGE',
		'INSERT'
	) AS v_RecordPopulated,
	-- *INF*: DECODE(TRUE,
	-- i_ExpirationDate   <=  lkp_EffectiveDate OR v_RecordPopulated = 'NOCHANGE', 'NOCHANGE',
	-- ISNULL(lkp_SupClassificationEPLIId) 
	-- OR ( i_RatingStateCode = lkp_RatingStateCode
	-- AND i_ClassCode = lkp_ClassCode
	-- AND i_OriginatingOrganizationCode = lkp_OriginatingOrganizationCode   
	-- AND (i_ClassDescription <>lkp_ClassDescription    
	--     OR i_ExpirationDate <> lkp_ExpirationDate
	--     OR  i_EffectiveDate <> lkp_EffectiveDate  )
	-- ),'INSERT',
	-- i_RatingStateCode<>lkp_RatingStateCode OR
	-- i_ClassCode<>lkp_ClassCode OR 
	-- i_OriginatingOrganizationCode <> lkp_OriginatingOrganizationCode,
	-- 'UPDATE',
	-- 'NOCHANGE'
	-- )
	DECODE(TRUE,
		i_ExpirationDate <= lkp_EffectiveDate 
		OR v_RecordPopulated = 'NOCHANGE', 'NOCHANGE',
		lkp_SupClassificationEPLIId IS NULL 
		OR ( i_RatingStateCode = lkp_RatingStateCode 
			AND i_ClassCode = lkp_ClassCode 
			AND i_OriginatingOrganizationCode = lkp_OriginatingOrganizationCode 
			AND ( i_ClassDescription <> lkp_ClassDescription 
				OR i_ExpirationDate <> lkp_ExpirationDate 
				OR i_EffectiveDate <> lkp_EffectiveDate 
			) 
		), 'INSERT',
		i_RatingStateCode <> lkp_RatingStateCode 
		OR i_ClassCode <> lkp_ClassCode 
		OR i_OriginatingOrganizationCode <> lkp_OriginatingOrganizationCode, 'UPDATE',
		'NOCHANGE'
	) AS v_ChangeFlag,
	v_ChangeFlag AS o_ChangeFlag,
	1 AS o_CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemID,
	sysdate AS o_CreatedDate,
	sysdate AS o_ModifiedDate,
	i_LineOfBusinessAbbreviation AS o_LineOfBusinessAbbreviation,
	i_RatingStateCode AS o_RatingStateCode,
	i_EffectiveDate AS o_EffectiveDate,
	i_ExpirationDate AS o_ExpirationDate,
	i_ClassCode AS o_ClassCode,
	i_ClassDescription AS o_ClassDescription,
	i_OriginatingOrganizationCode AS o_OriginatingOrganizationCode
	FROM SQ_SupClassificationEPLI
	LEFT JOIN LKP_SupClassificationEPLI
	ON LKP_SupClassificationEPLI.RatingStateCode = SQ_SupClassificationEPLI.RatingStateCode AND LKP_SupClassificationEPLI.OriginatingOrganizationCode = SQ_SupClassificationEPLI.OriginatingOrganizationCode AND LKP_SupClassificationEPLI.ClassCode = SQ_SupClassificationEPLI.ClassCode
	LEFT JOIN LKP_SUPCLASSIFICATIONEPLI_CURRENTCHANGEFLAG LKP_SUPCLASSIFICATIONEPLI_CURRENTCHANGEFLAG_i_RatingStateCode_i_ClassCode_i_EffectiveDate_i_ClassDescription_i_OriginatingOrganizationCode
	ON LKP_SUPCLASSIFICATIONEPLI_CURRENTCHANGEFLAG_i_RatingStateCode_i_ClassCode_i_EffectiveDate_i_ClassDescription_i_OriginatingOrganizationCode.RatingStateCode = i_RatingStateCode
	AND LKP_SUPCLASSIFICATIONEPLI_CURRENTCHANGEFLAG_i_RatingStateCode_i_ClassCode_i_EffectiveDate_i_ClassDescription_i_OriginatingOrganizationCode.ClassCode = i_ClassCode
	AND LKP_SUPCLASSIFICATIONEPLI_CURRENTCHANGEFLAG_i_RatingStateCode_i_ClassCode_i_EffectiveDate_i_ClassDescription_i_OriginatingOrganizationCode.EffectiveDate = i_EffectiveDate
	AND LKP_SUPCLASSIFICATIONEPLI_CURRENTCHANGEFLAG_i_RatingStateCode_i_ClassCode_i_EffectiveDate_i_ClassDescription_i_OriginatingOrganizationCode.ClassDescription = i_ClassDescription
	AND LKP_SUPCLASSIFICATIONEPLI_CURRENTCHANGEFLAG_i_RatingStateCode_i_ClassCode_i_EffectiveDate_i_ClassDescription_i_OriginatingOrganizationCode.OriginatingOrganizationCode = i_OriginatingOrganizationCode

),
RTR_Insert_Update AS (
	SELECT
	o_ChangeFlag AS ChangeFlag,
	o_CurrentSnapshotFlag AS CurrentSnapshotFlag,
	o_AuditId AS AuditId,
	o_SourceSystemID AS SourceSystemID,
	o_CreatedDate AS CreatedDate,
	o_ModifiedDate AS ModifiedDate,
	o_LineOfBusinessAbbreviation AS LineOfBusinessAbbreviation,
	o_RatingStateCode AS RatingStateCode,
	o_EffectiveDate AS EffectiveDate,
	o_ExpirationDate AS ExpirationDate,
	o_ClassCode AS ClassCode,
	o_ClassDescription AS ClassDescription,
	o_OriginatingOrganizationCode AS OriginatingOrganizationCode
	FROM EXP_Detect_Changes
),
RTR_Insert_Update_INSERT AS (SELECT * FROM RTR_Insert_Update WHERE ChangeFlag='INSERT'   OR  ChangeFlag='UPDATE'),
SupClassificationEPLI_IL AS (
	INSERT INTO SupClassificationEPLI
	(CurrentSnapshotFlag, AuditId, EffectiveDate, ExpirationDate, SourceSystemId, CreatedDate, ModifiedDate, LineOfBusinessAbbreviation, RatingStateCode, ClassCode, ClassDescription, OriginatingOrganizationCode)
	SELECT 
	CURRENTSNAPSHOTFLAG, 
	AUDITID, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	SourceSystemID AS SOURCESYSTEMID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	LINEOFBUSINESSABBREVIATION, 
	RATINGSTATECODE, 
	CLASSCODE, 
	CLASSDESCRIPTION, 
	ORIGINATINGORGANIZATIONCODE
	FROM RTR_Insert_Update_INSERT
),
SQ_SupClassificationEPLI_CheckExpDate AS (
	SELECT SupClassificationEPLI.SupClassificationEPLIId, 
	SupClassificationEPLI.EffectiveDate, 
	SupClassificationEPLI.ExpirationDate, 
	SupClassificationEPLI.LineOfBusinessAbbreviation, 
	SupClassificationEPLI.RatingStateCode, 
	SupClassificationEPLI.ClassCode, 
	SupClassificationEPLI.ClassDescription, 
	SupClassificationEPLI.OriginatingOrganizationCode 
	FROM
	@{pipeline().parameters.TARGET_TABLE_OWNER}.SupClassificationEPLI
	where CurrentSnapshotFlag = 1
	ORDER BY SupClassificationEPLI.ClassCode,
	SupClassificationEPLI.RatingStateCode, 
	SupClassificationEPLI.EffectiveDate DESC,
	SupClassificationEPLI.CreatedDate DESC
),
EXP_Lag_Eff_Dates AS (
	SELECT
	SupClassificationEPLIId,
	EffectiveDate,
	ExpirationDate,
	LineOfBusinessAbbreviation,
	RatingStateCode,
	ClassCode,
	ClassDescription,
	OriginatingOrganizationCode,
	-- *INF*: DECODE(TRUE,
	-- RatingStateCode = v_PREV_ROW_RatingStateCode
	--  AND ClassCode = v_PREV_ROW_ClassCode
	--  AND OriginatingOrganizationCode = v_PREV_ROW_OriginatingOrganizationCode
	--  AND 
	-- 		(ClassDescription <> v_PREV_ROW_ClassDescription
	-- 		 OR ADD_TO_DATE(ExpirationDate,'SS',+1) <> v_PREV_ROW_EffectiveDate)
	-- ,'0','1')
	DECODE(TRUE,
		RatingStateCode = v_PREV_ROW_RatingStateCode 
		AND ClassCode = v_PREV_ROW_ClassCode 
		AND OriginatingOrganizationCode = v_PREV_ROW_OriginatingOrganizationCode 
		AND ( ClassDescription <> v_PREV_ROW_ClassDescription 
			OR DATEADD(SECOND,+ 1,ExpirationDate) <> v_PREV_ROW_EffectiveDate 
		), '0',
		'1'
	) AS v_CurrentSnapshotFlag,
	-- *INF*: ADD_TO_DATE(
	-- IIF(v_PREV_ROW_EffectiveDate = TO_DATE('1800-01-01 00:00:00' , 'YYYY-MM-DD HH24:MI:SS' ),SYSDATE,v_PREV_ROW_EffectiveDate),'SS',-1)
	DATEADD(SECOND,- 1,IFF(v_PREV_ROW_EffectiveDate = TO_DATE('1800-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS'
		),
		SYSDATE,
		v_PREV_ROW_EffectiveDate
	)) AS v_ClassExpirationDate,
	v_CurrentSnapshotFlag AS o_CurrentSnapshotFlag,
	v_ClassExpirationDate AS o_ClassExpirationDate,
	EffectiveDate AS v_PREV_ROW_EffectiveDate,
	ExpirationDate AS v_PREV_ROW_ExpirationDate,
	LineOfBusinessAbbreviation AS v_PREV_ROW_LineOfBusinessAbbreviation,
	RatingStateCode AS v_PREV_ROW_RatingStateCode,
	ClassCode AS v_PREV_ROW_ClassCode,
	ClassDescription AS v_PREV_ROW_ClassDescription,
	OriginatingOrganizationCode AS v_PREV_ROW_OriginatingOrganizationCode,
	SYSDATE AS ModifiedDate
	FROM SQ_SupClassificationEPLI_CheckExpDate
),
FIL_FirstRowInAKGroup AS (
	SELECT
	SupClassificationEPLIId, 
	o_CurrentSnapshotFlag AS CurrentSnapshotFlag, 
	o_ClassExpirationDate AS ClassExpirationDate, 
	ModifiedDate
	FROM EXP_Lag_Eff_Dates
	WHERE CurrentSnapshotFlag = '0'
),
UPD_SupClassificationEPLI AS (
	SELECT
	SupClassificationEPLIId, 
	CurrentSnapshotFlag, 
	ClassExpirationDate AS ExpirationDate, 
	ModifiedDate
	FROM FIL_FirstRowInAKGroup
),
SupClassificationEPLI_IL_CheckExpDate AS (
	MERGE INTO SupClassificationEPLI AS T
	USING UPD_SupClassificationEPLI AS S
	ON T.SupClassificationEPLIId = S.SupClassificationEPLIId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.CurrentSnapshotFlag = S.CurrentSnapshotFlag, T.ExpirationDate = S.ExpirationDate, T.ModifiedDate = S.ModifiedDate
),