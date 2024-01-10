WITH
LKP_SupClassificationUmbrella_CurrentChangeFlag AS (
	SELECT
	SupClassificationUmbrellaId,
	EffectiveDate,
	ExpirationDate,
	RatingStateCode,
	ClassCode,
	ClassDescription,
	OriginatingOrganizationCode
	FROM (
		SELECT 
			SupClassificationUmbrellaId,
			EffectiveDate,
			ExpirationDate,
			RatingStateCode,
			ClassCode,
			ClassDescription,
			OriginatingOrganizationCode
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.SupClassificationUmbrella
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY RatingStateCode,ClassCode,EffectiveDate,ClassDescription,OriginatingOrganizationCode ORDER BY SupClassificationUmbrellaId) = 1
),
SQ_SupClassificationUmbrella AS (
	SELECT
		SupClassificationUmbrellaId,
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
	FROM SupClassificationUmbrella
),
LKP_SupClassificationUmbrella AS (
	SELECT
	SupClassificationUmbrellaId,
	EffectiveDate,
	ExpirationDate,
	RatingStateCode,
	ClassCode,
	ClassDescription,
	OriginatingOrganizationCode
	FROM (
		SELECT 
			SupClassificationUmbrellaId,
			EffectiveDate,
			ExpirationDate,
			RatingStateCode,
			ClassCode,
			ClassDescription,
			OriginatingOrganizationCode
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.SupClassificationUmbrella
		WHERE CurrentSnapshotFlag =1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY RatingStateCode,OriginatingOrganizationCode,ClassCode ORDER BY SupClassificationUmbrellaId) = 1
),
EXP_Detect_Changes AS (
	SELECT
	SQ_SupClassificationUmbrella.EffectiveDate AS i_EffectiveDate,
	SQ_SupClassificationUmbrella.ExpirationDate AS i_ExpirationDate,
	SQ_SupClassificationUmbrella.LineOfBusinessAbbreviation AS i_LineOfBusinessAbbreviation,
	SQ_SupClassificationUmbrella.RatingStateCode AS i_RatingStateCode,
	SQ_SupClassificationUmbrella.ClassCode AS i_ClassCode,
	SQ_SupClassificationUmbrella.ClassDescription AS i_ClassDescription,
	SQ_SupClassificationUmbrella.OriginatingOrganizationCode AS i_OriginatingOrganizationCode,
	LKP_SupClassificationUmbrella.SupClassificationUmbrellaId AS lkp_SupClassificationUmbrellaId,
	LKP_SupClassificationUmbrella.EffectiveDate AS lkp_EffectiveDate,
	LKP_SupClassificationUmbrella.ExpirationDate AS lkp_ExpirationDate,
	LKP_SupClassificationUmbrella.RatingStateCode AS lkp_RatingStateCode,
	LKP_SupClassificationUmbrella.ClassCode AS lkp_ClassCode,
	LKP_SupClassificationUmbrella.ClassDescription AS lkp_ClassDescription,
	LKP_SupClassificationUmbrella.OriginatingOrganizationCode AS lkp_OriginatingOrganizationCode,
	-- *INF*: DECODE(TRUE,
	-- NOT ISNULL(:LKP.LKP_SUPCLASSIFICATIONUMBRELLA_CURRENTCHANGEFLAG(i_RatingStateCode,i_ClassCode,i_EffectiveDate,i_ClassDescription,i_OriginatingOrganizationCode)),
	-- 'NOCHANGE',
	-- 'INSERT')						
	DECODE(TRUE,
		NOT LKP_SUPCLASSIFICATIONUMBRELLA_CURRENTCHANGEFLAG_i_RatingStateCode_i_ClassCode_i_EffectiveDate_i_ClassDescription_i_OriginatingOrganizationCode.SupClassificationUmbrellaId IS NULL, 'NOCHANGE',
		'INSERT') AS v_RecordPopulated,
	-- *INF*: DECODE(TRUE,
	-- i_ExpirationDate   <=  lkp_EffectiveDate OR v_RecordPopulated = 'NOCHANGE', 'NOCHANGE',
	-- ISNULL(lkp_SupClassificationUmbrellaId) 
	-- OR  ( i_RatingStateCode = lkp_RatingStateCode
	-- 	AND i_ClassCode = lkp_ClassCode
	-- 	AND i_OriginatingOrganizationCode = lkp_OriginatingOrganizationCode   
	-- 	AND (i_ClassDescription <>lkp_ClassDescription    
	--    			 OR i_ExpirationDate <> lkp_ExpirationDate
	--    			 OR  i_EffectiveDate <> lkp_EffectiveDate  )
	-- ),'INSERT',  
	-- i_RatingStateCode<>lkp_RatingStateCode OR
	-- i_ClassCode<>lkp_ClassCode OR 
	-- i_OriginatingOrganizationCode <> lkp_OriginatingOrganizationCode ,
	-- 'UPDATE',
	-- 'NOCHANGE'
	-- )
	DECODE(TRUE,
		i_ExpirationDate <= lkp_EffectiveDate OR v_RecordPopulated = 'NOCHANGE', 'NOCHANGE',
		lkp_SupClassificationUmbrellaId IS NULL OR ( i_RatingStateCode = lkp_RatingStateCode AND i_ClassCode = lkp_ClassCode AND i_OriginatingOrganizationCode = lkp_OriginatingOrganizationCode AND ( i_ClassDescription <> lkp_ClassDescription OR i_ExpirationDate <> lkp_ExpirationDate OR i_EffectiveDate <> lkp_EffectiveDate ) ), 'INSERT',
		i_RatingStateCode <> lkp_RatingStateCode OR i_ClassCode <> lkp_ClassCode OR i_OriginatingOrganizationCode <> lkp_OriginatingOrganizationCode, 'UPDATE',
		'NOCHANGE') AS v_ChangeFlag,
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
	FROM SQ_SupClassificationUmbrella
	LEFT JOIN LKP_SupClassificationUmbrella
	ON LKP_SupClassificationUmbrella.RatingStateCode = SQ_SupClassificationUmbrella.RatingStateCode AND LKP_SupClassificationUmbrella.OriginatingOrganizationCode = SQ_SupClassificationUmbrella.OriginatingOrganizationCode AND LKP_SupClassificationUmbrella.ClassCode = SQ_SupClassificationUmbrella.ClassCode
	LEFT JOIN LKP_SUPCLASSIFICATIONUMBRELLA_CURRENTCHANGEFLAG LKP_SUPCLASSIFICATIONUMBRELLA_CURRENTCHANGEFLAG_i_RatingStateCode_i_ClassCode_i_EffectiveDate_i_ClassDescription_i_OriginatingOrganizationCode
	ON LKP_SUPCLASSIFICATIONUMBRELLA_CURRENTCHANGEFLAG_i_RatingStateCode_i_ClassCode_i_EffectiveDate_i_ClassDescription_i_OriginatingOrganizationCode.RatingStateCode = i_RatingStateCode
	AND LKP_SUPCLASSIFICATIONUMBRELLA_CURRENTCHANGEFLAG_i_RatingStateCode_i_ClassCode_i_EffectiveDate_i_ClassDescription_i_OriginatingOrganizationCode.ClassCode = i_ClassCode
	AND LKP_SUPCLASSIFICATIONUMBRELLA_CURRENTCHANGEFLAG_i_RatingStateCode_i_ClassCode_i_EffectiveDate_i_ClassDescription_i_OriginatingOrganizationCode.EffectiveDate = i_EffectiveDate
	AND LKP_SUPCLASSIFICATIONUMBRELLA_CURRENTCHANGEFLAG_i_RatingStateCode_i_ClassCode_i_EffectiveDate_i_ClassDescription_i_OriginatingOrganizationCode.ClassDescription = i_ClassDescription
	AND LKP_SUPCLASSIFICATIONUMBRELLA_CURRENTCHANGEFLAG_i_RatingStateCode_i_ClassCode_i_EffectiveDate_i_ClassDescription_i_OriginatingOrganizationCode.OriginatingOrganizationCode = i_OriginatingOrganizationCode

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
SupClassificationUmbrella_IL AS (
	INSERT INTO SupClassificationUmbrella
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
SQ_SupClassificationUmbrella_CheckExpDate AS (
	SELECT SupClassificationUmbrella.SupClassificationUmbrellaId, SupClassificationUmbrella.EffectiveDate, 
	SupClassificationUmbrella.ExpirationDate, SupClassificationUmbrella.LineOfBusinessAbbreviation, SupClassificationUmbrella.RatingStateCode, 
	SupClassificationUmbrella.ClassCode, 
	SupClassificationUmbrella.ClassDescription, SupClassificationUmbrella.OriginatingOrganizationCode 
	FROM
	@{pipeline().parameters.TARGET_TABLE_OWNER}.SupClassificationUmbrella
	where CurrentSnapshotFlag = 1
	ORDER BY SupClassificationUmbrella.ClassCode,
	SupClassificationUmbrella.RatingStateCode,
	SupClassificationUmbrella.EffectiveDate DESC,
	SupClassificationUmbrella.CreatedDate DESC
),
EXP_Lag_Eff_Dates AS (
	SELECT
	SupClassificationUmbrellaId,
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
		RatingStateCode = v_PREV_ROW_RatingStateCode AND ClassCode = v_PREV_ROW_ClassCode AND OriginatingOrganizationCode = v_PREV_ROW_OriginatingOrganizationCode AND ( ClassDescription <> v_PREV_ROW_ClassDescription OR ADD_TO_DATE(ExpirationDate, 'SS', + 1) <> v_PREV_ROW_EffectiveDate ), '0',
		'1') AS v_CurrentSnapshotFlag,
	-- *INF*: ADD_TO_DATE(
	-- IIF(v_PREV_ROW_EffectiveDate = TO_DATE('1800-01-01 00:00:00' , 'YYYY-MM-DD HH24:MI:SS' ),SYSDATE,v_PREV_ROW_EffectiveDate)
	-- ,'SS',-1)
	ADD_TO_DATE(IFF(v_PREV_ROW_EffectiveDate = TO_DATE('1800-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS'), SYSDATE, v_PREV_ROW_EffectiveDate), 'SS', - 1) AS v_ClassExpirationDate,
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
	FROM SQ_SupClassificationUmbrella_CheckExpDate
),
FIL_FirstRowInAKGroup AS (
	SELECT
	SupClassificationUmbrellaId, 
	o_CurrentSnapshotFlag AS CurrentSnapshotFlag, 
	o_ClassExpirationDate AS ClassExpirationDate, 
	ModifiedDate
	FROM EXP_Lag_Eff_Dates
	WHERE CurrentSnapshotFlag = '0'
),
UPD_SupClassificationUmbrella AS (
	SELECT
	SupClassificationUmbrellaId, 
	CurrentSnapshotFlag, 
	ClassExpirationDate AS ExpirationDate, 
	ModifiedDate
	FROM FIL_FirstRowInAKGroup
),
SupClassificationUmbrella_IL_CheckExpDate AS (
	MERGE INTO SupClassificationUmbrella AS T
	USING UPD_SupClassificationUmbrella AS S
	ON T.SupClassificationUmbrellaId = S.SupClassificationUmbrellaId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.CurrentSnapshotFlag = S.CurrentSnapshotFlag, T.ExpirationDate = S.ExpirationDate, T.ModifiedDate = S.ModifiedDate
),