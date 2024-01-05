WITH
SQ_MedicalCauseCodeStage AS (
	SELECT
		MedicalCauseCodeStageId,
		code,
		short_descript,
		long_descript,
		med_class_code_type_id,
		ExtractDate,
		SourceSystemId
	FROM MedicalCauseCodeStage
),
EXP_Src_Values AS (
	SELECT
	code,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(code),'N/A',
	-- IS_SPACES(code),'N/A',
	-- LENGTH(code)=0,'N/A',
	-- LTRIM(RTRIM(code)))
	DECODE(TRUE,
	code IS NULL, 'N/A',
	IS_SPACES(code), 'N/A',
	LENGTH(code) = 0, 'N/A',
	LTRIM(RTRIM(code))) AS o_code,
	short_descript,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(short_descript),'N/A',
	-- IS_SPACES(short_descript),'N/A',
	-- LENGTH(short_descript)=0,'N/A',
	-- LTRIM(RTRIM(short_descript)))
	DECODE(TRUE,
	short_descript IS NULL, 'N/A',
	IS_SPACES(short_descript), 'N/A',
	LENGTH(short_descript) = 0, 'N/A',
	LTRIM(RTRIM(short_descript))) AS o_short_descript,
	long_descript,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(long_descript),'N/A',
	-- IS_SPACES(long_descript),'N/A',
	-- LENGTH(long_descript)=0,'N/A',
	-- LTRIM(RTRIM(long_descript)))
	DECODE(TRUE,
	long_descript IS NULL, 'N/A',
	IS_SPACES(long_descript), 'N/A',
	LENGTH(long_descript) = 0, 'N/A',
	LTRIM(RTRIM(long_descript))) AS o_long_descript,
	med_class_code_type_id
	FROM SQ_MedicalCauseCodeStage
),
LKP_SupMedicalClassCodeTypeStage AS (
	SELECT
	code_type,
	med_class_code_type_id
	FROM (
		SELECT 
		S.code_type as code_type, 
		S.med_class_code_type_id as med_class_code_type_id 
		
		FROM 
		@{pipeline().parameters.SOURCE_TABLE_OWNER}.SupMedicalClassCodeTypeStage S
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY med_class_code_type_id ORDER BY code_type) = 1
),
LKP_SupMedicalClassCodeType AS (
	SELECT
	SupMedicalClassCodeTypeId,
	MedicalClassCodeType
	FROM (
		SELECT 
		S.SupMedicalClassCodeTypeId as SupMedicalClassCodeTypeId, 
		S.MedicalClassCodeType as MedicalClassCodeType 
		
		FROM 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.SupMedicalClassCodeType S
		
		where
		S.CurrentSnapshotFlag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY MedicalClassCodeType ORDER BY SupMedicalClassCodeTypeId) = 1
),
LKP_SupMedicalCauseCode AS (
	SELECT
	SupMedicalCauseCodeId,
	SupMedicalClassCodeTypeId,
	MedicalCauseCode,
	ShortDescription,
	LongDescription
	FROM (
		SELECT 
		S.SupMedicalClassCodeTypeId as SupMedicalClassCodeTypeId, 
		S.ShortDescription as ShortDescription, 
		S.LongDescription as LongDescription, S.SupMedicalCauseCodeId as SupMedicalCauseCodeId, S.MedicalCauseCode as MedicalCauseCode
		
		 FROM 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.SupMedicalCauseCode S
		
		where
		S.CurrentSnapshotFlag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY SupMedicalClassCodeTypeId,MedicalCauseCode ORDER BY SupMedicalCauseCodeId) = 1
),
EXP_TargetLkp_Detect_Changes AS (
	SELECT
	LKP_SupMedicalCauseCode.SupMedicalCauseCodeId AS Lkp_SupMedicalCauseCodeId,
	LKP_SupMedicalCauseCode.SupMedicalClassCodeTypeId AS Lkp_SupMedicalClassCodeTypeId,
	LKP_SupMedicalCauseCode.MedicalCauseCode AS Lkp_MedicalCauseCode,
	LKP_SupMedicalCauseCode.ShortDescription AS Lkp_ShortDescription,
	LKP_SupMedicalCauseCode.LongDescription AS Lkp_LongDescription,
	-- *INF*: iif(isnull(Lkp_SupMedicalCauseCodeId),'NEW',
	-- 
	--   iif(
	--    
	--        ltrim(rtrim(Lkp_SupMedicalClassCodeTypeId)) != ltrim(rtrim(SupMedicalClassCodeTypeId))
	-- 
	-- or
	-- 
	--        ltrim(rtrim(Lkp_MedicalCauseCode)) != ltrim(rtrim(MedicalCauseCode))
	-- 
	-- or
	-- 
	--        ltrim(rtrim(Lkp_ShortDescription)) != ltrim(rtrim(ShortDescription))
	-- 
	-- or
	-- 
	--        ltrim(rtrim(Lkp_LongDescription)) != ltrim(rtrim(LongDescription)),
	-- 
	--    'UPDATE', 'NOCHANGE' )
	-- 
	--    )
	IFF(Lkp_SupMedicalCauseCodeId IS NULL, 'NEW', IFF(ltrim(rtrim(Lkp_SupMedicalClassCodeTypeId)) != ltrim(rtrim(SupMedicalClassCodeTypeId)) OR ltrim(rtrim(Lkp_MedicalCauseCode)) != ltrim(rtrim(MedicalCauseCode)) OR ltrim(rtrim(Lkp_ShortDescription)) != ltrim(rtrim(ShortDescription)) OR ltrim(rtrim(Lkp_LongDescription)) != ltrim(rtrim(LongDescription)), 'UPDATE', 'NOCHANGE')) AS v_ChangedFlag,
	v_ChangedFlag AS ChangedFlag,
	-- *INF*: iif(v_ChangedFlag='NEW',
	-- 	to_date('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'),sysdate)
	IFF(v_ChangedFlag = 'NEW', to_date('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'), sysdate) AS EffectiveDate,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS ExpirationDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS SourceSystemId,
	SYSDATE AS CreatedDate,
	SYSDATE AS ModifiedDate,
	EXP_Src_Values.o_code AS MedicalCauseCode,
	EXP_Src_Values.o_short_descript AS ShortDescription,
	EXP_Src_Values.o_long_descript AS LongDescription,
	1 AS CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditId,
	LKP_SupMedicalClassCodeType.SupMedicalClassCodeTypeId
	FROM EXP_Src_Values
	LEFT JOIN LKP_SupMedicalCauseCode
	ON LKP_SupMedicalCauseCode.SupMedicalClassCodeTypeId = LKP_SupMedicalClassCodeType.SupMedicalClassCodeTypeId AND LKP_SupMedicalCauseCode.MedicalCauseCode = EXP_Src_Values.o_code
	LEFT JOIN LKP_SupMedicalClassCodeType
	ON LKP_SupMedicalClassCodeType.MedicalClassCodeType = LKP_SupMedicalClassCodeTypeStage.code_type
),
FIL_Lkp_Records AS (
	SELECT
	ChangedFlag, 
	EffectiveDate, 
	ExpirationDate, 
	SourceSystemId, 
	CreatedDate, 
	ModifiedDate, 
	MedicalCauseCode, 
	ShortDescription, 
	LongDescription, 
	CurrentSnapshotFlag, 
	AuditId, 
	SupMedicalClassCodeTypeId
	FROM EXP_TargetLkp_Detect_Changes
	WHERE ChangedFlag = 'NEW' or ChangedFlag =  'UPDATE'
),
SupMedicalCauseCode_Insert AS (
	INSERT INTO SupMedicalCauseCode
	(SupMedicalClassCodeTypeId, CurrentSnapshotFlag, AuditId, EffectiveDate, ExpirationDate, SourceSystemId, CreatedDate, ModifiedDate, MedicalCauseCode, ShortDescription, LongDescription)
	SELECT 
	SUPMEDICALCLASSCODETYPEID, 
	CURRENTSNAPSHOTFLAG, 
	AUDITID, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	SOURCESYSTEMID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	MEDICALCAUSECODE, 
	SHORTDESCRIPTION, 
	LONGDESCRIPTION
	FROM FIL_Lkp_Records
),
SQ_SupMedicalCauseCode AS (
	SELECT
	A.SupMedicalCauseCodeId,
	A.EffectiveDate,
	A.ExpirationDate,
	A.MedicalCauseCode 
	
	FROM
	 @{pipeline().parameters.TARGET_TABLE_OWNER}.SupMedicalCauseCode A
	
	WHERE Exists 
	    ( 
	SELECT 1
	FROM
	@{pipeline().parameters.TARGET_TABLE_OWNER}.SupMedicalCauseCode B
	
	WHERE
	B.CurrentSnapshotFlag = 1
	AND
	A.MedicalCauseCode = B.MedicalCauseCode
	
	GROUP BY 
	B.MedicalCauseCode 
	
	HAVING 
	COUNT(*) > 1
	       )
	
	AND
	A.CurrentSnapshotFlag = 1
	
	ORDER BY
	A.MedicalCauseCode, 
	A.EffectiveDate Desc
),
EXP_Lag_ExpirationDate AS (
	SELECT
	SupMedicalCauseCodeId,
	EffectiveDate,
	ExpirationDate AS orig_ExpirationDate,
	-- *INF*: DECODE(TRUE,
	-- 	MedicalCauseCode= v_PREV_ROW_MedicalCauseCode, ADD_TO_DATE(v_PREV_ROW_EffectiveDate,'SS',-1),
	-- 	orig_ExpirationDate)
	DECODE(TRUE,
	MedicalCauseCode = v_PREV_ROW_MedicalCauseCode, ADD_TO_DATE(v_PREV_ROW_EffectiveDate, 'SS', - 1),
	orig_ExpirationDate) AS v_ExpirationDate,
	v_ExpirationDate AS ExpirationDate,
	0 AS CurrentSnapshotFlag,
	MedicalCauseCode,
	EffectiveDate AS v_PREV_ROW_EffectiveDate,
	MedicalCauseCode AS v_PREV_ROW_MedicalCauseCode,
	SYSDATE AS ModifiedDate
	FROM SQ_SupMedicalCauseCode
),
FIL_FirstRowAkId AS (
	SELECT
	SupMedicalCauseCodeId, 
	orig_ExpirationDate, 
	ExpirationDate, 
	CurrentSnapshotFlag, 
	ModifiedDate
	FROM EXP_Lag_ExpirationDate
	WHERE orig_ExpirationDate != ExpirationDate
),
UPD_SupMedicalCauseCode AS (
	SELECT
	SupMedicalCauseCodeId, 
	ExpirationDate, 
	CurrentSnapshotFlag, 
	ModifiedDate
	FROM FIL_FirstRowAkId
),
SupMedicalCauseCode_Update AS (
	MERGE INTO SupMedicalCauseCode AS T
	USING UPD_SupMedicalCauseCode AS S
	ON T.SupMedicalCauseCodeId = S.SupMedicalCauseCodeId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.CurrentSnapshotFlag = S.CurrentSnapshotFlag, T.ExpirationDate = S.ExpirationDate, T.ModifiedDate = S.ModifiedDate
),