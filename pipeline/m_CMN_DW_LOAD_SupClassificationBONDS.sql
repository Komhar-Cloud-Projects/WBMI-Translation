WITH
LKP_SupClassificationBonds_CurrentChangeFlag AS (
	SELECT
	SupClassificationBondsId,
	EffectiveDate,
	RatingStateCode,
	ClassCode,
	ClassDescription,
	OriginatingOrganizationCode
	FROM (
		SELECT 
			SupClassificationBondsId,
			EffectiveDate,
			RatingStateCode,
			ClassCode,
			ClassDescription,
			OriginatingOrganizationCode
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.SupClassificationBonds
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY EffectiveDate,RatingStateCode,ClassCode,ClassDescription,OriginatingOrganizationCode ORDER BY SupClassificationBondsId) = 1
),
SQ_SupClassificationBonds AS (
	SELECT
		SupClassificationBondsId,
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
	FROM SupClassificationBonds
	INNER JOIN SupClassificationBonds
),
LKP_SupClassificationBonds AS (
	SELECT
	SupClassificationBondsId,
	CurrentSnapshotFlag,
	AuditId,
	EffectiveDate,
	ExpirationDate,
	SourceSystemId,
	CreatedDate,
	ModifiedDate,
	LineOfBusinessAbbreviation,
	RatingStateCode,
	ClassCode,
	ClassDescription,
	OriginatingOrganizationCode
	FROM (
		SELECT 
			SupClassificationBondsId,
			CurrentSnapshotFlag,
			AuditId,
			EffectiveDate,
			ExpirationDate,
			SourceSystemId,
			CreatedDate,
			ModifiedDate,
			LineOfBusinessAbbreviation,
			RatingStateCode,
			ClassCode,
			ClassDescription,
			OriginatingOrganizationCode
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.SupClassificationBonds
		WHERE CurrentSnapshotFlag =1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY RatingStateCode,ClassCode,OriginatingOrganizationCode ORDER BY SupClassificationBondsId) = 1
),
EXP_Detect_Changes AS (
	SELECT
	SQ_SupClassificationBonds.LineOfBusinessAbbreviation AS i_LineOfBusinessAbbreviation,
	SQ_SupClassificationBonds.RatingStateCode AS i_RatingStateCode,
	SQ_SupClassificationBonds.EffectiveDate AS i_EffectiveDate,
	SQ_SupClassificationBonds.ExpirationDate AS i_ExpirationDate,
	SQ_SupClassificationBonds.ClassCode AS i_ClassCode,
	SQ_SupClassificationBonds.ClassDescription AS i_ClassDescription,
	SQ_SupClassificationBonds.OriginatingOrganizationCode AS i_OriginatingOrganizationCode,
	LKP_SupClassificationBonds.SupClassificationBondsId AS lkp_SupClassificationBondsId,
	LKP_SupClassificationBonds.EffectiveDate AS lkp_EffectiveDate,
	LKP_SupClassificationBonds.ExpirationDate AS lkp_ExpirationDate,
	LKP_SupClassificationBonds.LineOfBusinessAbbreviation AS lkp_LineOfBusinessAbbreviation,
	LKP_SupClassificationBonds.RatingStateCode AS lkp_RatingStateCode,
	LKP_SupClassificationBonds.ClassCode AS lkp_ClassCode,
	LKP_SupClassificationBonds.ClassDescription AS lkp_ClassDescription,
	LKP_SupClassificationBonds.OriginatingOrganizationCode AS lkp_OriginatingOrganizationCode,
	-- *INF*: DECODE(TRUE,NOT ISNULL(:LKP.LKP_SUPCLASSIFICATIONBONDS_CURRENTCHANGEFLAG(i_EffectiveDate,i_RatingStateCode,i_ClassCode,i_ClassDescription,i_OriginatingOrganizationCode)),'NOCHANGE','INSERT')
	DECODE(TRUE,
		NOT LKP_SUPCLASSIFICATIONBONDS_CURRENTCHANGEFLAG_i_EffectiveDate_i_RatingStateCode_i_ClassCode_i_ClassDescription_i_OriginatingOrganizationCode.SupClassificationBondsId IS NULL, 'NOCHANGE',
		'INSERT') AS v_RecordPopulated,
	-- *INF*: DECODE(TRUE,
	-- i_ExpirationDate <= lkp_EffectiveDate OR v_RecordPopulated = 'NOCHANGE', 'NOCHANGE',
	-- ISNULL(lkp_SupClassificationBondsId) OR 
	-- (i_RatingStateCode = lkp_RatingStateCode 
	-- AND i_ClassCode = lkp_ClassCode 
	-- AND i_OriginatingOrganizationCode = lkp_OriginatingOrganizationCode
	-- AND 
	-- 	(i_EffectiveDate <> lkp_EffectiveDate 
	-- 	OR i_ExpirationDate <> lkp_ExpirationDate
	-- 	OR i_ClassDescription <> lkp_ClassDescription)
	-- ),'INSERT',
	-- i_RatingStateCode <> lkp_RatingStateCode OR 
	-- i_ClassCode <> lkp_ClassCode OR
	-- i_OriginatingOrganizationCode <> lkp_OriginatingOrganizationCode,
	-- 'UPDATE',
	-- 'NOCHANGE'
	-- )
	DECODE(TRUE,
		i_ExpirationDate <= lkp_EffectiveDate OR v_RecordPopulated = 'NOCHANGE', 'NOCHANGE',
		lkp_SupClassificationBondsId IS NULL OR ( i_RatingStateCode = lkp_RatingStateCode AND i_ClassCode = lkp_ClassCode AND i_OriginatingOrganizationCode = lkp_OriginatingOrganizationCode AND ( i_EffectiveDate <> lkp_EffectiveDate OR i_ExpirationDate <> lkp_ExpirationDate OR i_ClassDescription <> lkp_ClassDescription ) ), 'INSERT',
		i_RatingStateCode <> lkp_RatingStateCode OR i_ClassCode <> lkp_ClassCode OR i_OriginatingOrganizationCode <> lkp_OriginatingOrganizationCode, 'UPDATE',
		'NOCHANGE') AS v_ChangeFlag,
	v_ChangeFlag AS o_ChangeFlag,
	1 AS o_CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	i_EffectiveDate AS o_EffectiveDate,
	i_ExpirationDate AS o_ExpirationDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	SYSDATE AS o_CreatedDate,
	SYSDATE AS o_ModifiedDate,
	i_LineOfBusinessAbbreviation AS o_LineOfBusinessAbbreviation,
	i_RatingStateCode AS o_RatingStateCode,
	i_ClassCode AS o_ClassCode,
	i_ClassDescription AS o_ClassDescription,
	i_OriginatingOrganizationCode AS o_OriginatingOrganizationCode
	FROM SQ_SupClassificationBonds
	LEFT JOIN LKP_SupClassificationBonds
	ON LKP_SupClassificationBonds.RatingStateCode = SQ_SupClassificationBonds.RatingStateCode AND LKP_SupClassificationBonds.ClassCode = SQ_SupClassificationBonds.ClassCode AND LKP_SupClassificationBonds.OriginatingOrganizationCode = SQ_SupClassificationBonds.OriginatingOrganizationCode
	LEFT JOIN LKP_SUPCLASSIFICATIONBONDS_CURRENTCHANGEFLAG LKP_SUPCLASSIFICATIONBONDS_CURRENTCHANGEFLAG_i_EffectiveDate_i_RatingStateCode_i_ClassCode_i_ClassDescription_i_OriginatingOrganizationCode
	ON LKP_SUPCLASSIFICATIONBONDS_CURRENTCHANGEFLAG_i_EffectiveDate_i_RatingStateCode_i_ClassCode_i_ClassDescription_i_OriginatingOrganizationCode.EffectiveDate = i_EffectiveDate
	AND LKP_SUPCLASSIFICATIONBONDS_CURRENTCHANGEFLAG_i_EffectiveDate_i_RatingStateCode_i_ClassCode_i_ClassDescription_i_OriginatingOrganizationCode.RatingStateCode = i_RatingStateCode
	AND LKP_SUPCLASSIFICATIONBONDS_CURRENTCHANGEFLAG_i_EffectiveDate_i_RatingStateCode_i_ClassCode_i_ClassDescription_i_OriginatingOrganizationCode.ClassCode = i_ClassCode
	AND LKP_SUPCLASSIFICATIONBONDS_CURRENTCHANGEFLAG_i_EffectiveDate_i_RatingStateCode_i_ClassCode_i_ClassDescription_i_OriginatingOrganizationCode.ClassDescription = i_ClassDescription
	AND LKP_SUPCLASSIFICATIONBONDS_CURRENTCHANGEFLAG_i_EffectiveDate_i_RatingStateCode_i_ClassCode_i_ClassDescription_i_OriginatingOrganizationCode.OriginatingOrganizationCode = i_OriginatingOrganizationCode

),
RT_Insert_Update AS (
	SELECT
	o_ChangeFlag AS ChangeFlag,
	o_CurrentSnapshotFlag AS CurrentSnapshotFlag,
	o_AuditId AS AuditId,
	o_EffectiveDate AS EffectiveDate,
	o_ExpirationDate AS ExpirationDate,
	o_SourceSystemId AS SourceSystemId,
	o_CreatedDate AS CreatedDate,
	o_ModifiedDate AS ModifiedDate,
	o_LineOfBusinessAbbreviation AS LineOfBusinessAbbreviation,
	o_RatingStateCode AS RatingStateCode,
	o_ClassCode AS ClassCode,
	o_ClassDescription AS ClassDescription,
	o_OriginatingOrganizationCode AS OriginatingOrganizationCode
	FROM EXP_Detect_Changes
),
RT_Insert_Update_INSERT AS (SELECT * FROM RT_Insert_Update WHERE ChangeFlag = 'INSERT' OR ChangeFlag = 'UPDATE'),
SupClassificationBonds AS (
	INSERT INTO SupClassificationBonds
	(CurrentSnapshotFlag, AuditId, EffectiveDate, ExpirationDate, SourceSystemId, CreatedDate, ModifiedDate, LineOfBusinessAbbreviation, RatingStateCode, ClassCode, ClassDescription, OriginatingOrganizationCode)
	SELECT 
	CURRENTSNAPSHOTFLAG, 
	AUDITID, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	SOURCESYSTEMID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	LINEOFBUSINESSABBREVIATION, 
	RATINGSTATECODE, 
	CLASSCODE, 
	CLASSDESCRIPTION, 
	ORIGINATINGORGANIZATIONCODE
	FROM RT_Insert_Update_INSERT
),
SQ_SupClassificationBonds_CheckExpDate AS (
	SELECT SupClassificationBonds.SupClassificationBondsId, SupClassificationBonds.EffectiveDate, SupClassificationBonds.ExpirationDate, SupClassificationBonds.LineOfBusinessAbbreviation, SupClassificationBonds.RatingStateCode, SupClassificationBonds.ClassCode, SupClassificationBonds.ClassDescription, SupClassificationBonds.OriginatingOrganizationCode 
	FROM
	@{pipeline().parameters.TARGET_TABLE_OWNER}.SupClassificationBonds
	where CurrentSnapshotFlag = 1
	ORDER BY SupClassificationBonds.ClassCode,
	SupClassificationBonds.RatingStateCode, 
	SupClassificationBonds.EffectiveDate DESC, 
	SupClassificationBonds.CreatedDate DESC
),
EXP_Lag_Eff_Dates AS (
	SELECT
	SupClassificationBondsId,
	LineOfBusinessAbbreviation,
	RatingStateCode,
	EffectiveDate,
	ExpirationDate,
	ClassCode,
	ClassDescription,
	OriginatingOrganizationCode,
	-- *INF*: DECODE(TRUE,
	-- RatingStateCode = v_PREV_ROW_RatingStateCode
	--  AND ClassCode = v_PREV_ROW_ClassCode
	--  AND OriginatingOrganizationCode = v_PREV_ROW_OriginatingOrganizationCode
	--  AND 
	-- 	(ClassDescription <> v_PREV_ROW_ClassDescription
	-- 	 OR ADD_TO_DATE(ExpirationDate,'SS',+1) <> v_PREV_ROW_EffectiveDate)
	-- ,'0','1')
	DECODE(TRUE,
		RatingStateCode = v_PREV_ROW_RatingStateCode AND ClassCode = v_PREV_ROW_ClassCode AND OriginatingOrganizationCode = v_PREV_ROW_OriginatingOrganizationCode AND ( ClassDescription <> v_PREV_ROW_ClassDescription OR ADD_TO_DATE(ExpirationDate, 'SS', + 1) <> v_PREV_ROW_EffectiveDate ), '0',
		'1') AS v_CurrentSnapshotFlag,
	-- *INF*: ADD_TO_DATE(
	-- IIF(v_PREV_ROW_EffectiveDate = TO_DATE('1800-01-01 00:00:00' , 'YYYY-MM-DD HH24:MI:SS' ),SYSDATE,v_PREV_ROW_EffectiveDate)
	-- ,'SS',-1)
	ADD_TO_DATE(IFF(v_PREV_ROW_EffectiveDate = TO_DATE('1800-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS'), SYSDATE, v_PREV_ROW_EffectiveDate), 'SS', - 1) AS v_ClassExpirationDate,
	v_CurrentSnapshotFlag AS o_CurrentSnapshotFlag,
	v_ClassExpirationDate AS o_ClassExpirationDate,
	LineOfBusinessAbbreviation AS v_PREV_ROW_LineOfBusinessAbbreviation,
	RatingStateCode AS v_PREV_ROW_RatingStateCode,
	EffectiveDate AS v_PREV_ROW_EffectiveDate,
	ExpirationDate AS v_PREV_ROW_ExpirationDate,
	ClassCode AS v_PREV_ROW_ClassCode,
	ClassDescription AS v_PREV_ROW_ClassDescription,
	OriginatingOrganizationCode AS v_PREV_ROW_OriginatingOrganizationCode,
	SYSDATE AS ModifiedDate
	FROM SQ_SupClassificationBonds_CheckExpDate
),
FIL_FirstRowInAKGroup AS (
	SELECT
	SupClassificationBondsId, 
	o_CurrentSnapshotFlag AS CurrentSnapshotFlag, 
	o_ClassExpirationDate AS ExpirationDate, 
	ModifiedDate
	FROM EXP_Lag_Eff_Dates
	WHERE CurrentSnapshotFlag = '0'
),
UPD_SupClassificationBonds AS (
	SELECT
	SupClassificationBondsId, 
	CurrentSnapshotFlag, 
	ExpirationDate, 
	ModifiedDate
	FROM FIL_FirstRowInAKGroup
),
SupClassificationBonds_CheckExpDate AS (
	MERGE INTO SupClassificationBonds AS T
	USING UPD_SupClassificationBonds AS S
	ON T.SupClassificationBondsId = S.SupClassificationBondsId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.CurrentSnapshotFlag = S.CurrentSnapshotFlag, T.ExpirationDate = S.ExpirationDate, T.ModifiedDate = S.ModifiedDate
),