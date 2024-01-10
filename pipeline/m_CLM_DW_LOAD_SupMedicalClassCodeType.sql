WITH
SQ_SupMedicalClassCodeTypeStage AS (
	SELECT SupMedicalClassCodeTypeStage.code_type, SupMedicalClassCodeTypeStage.descript 
	FROM
	 SupMedicalClassCodeTypeStage
),
EXP_Src_Value AS (
	SELECT
	code_type,
	-- *INF*: iif(isnull(code_type),-1,code_type)
	IFF(code_type IS NULL, - 1, code_type) AS o_code_type,
	descript,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(descript),'N/A',
	-- IS_SPACES(descript),'N/A',
	-- LENGTH(descript)=0,'N/A',
	-- LTRIM(RTRIM(descript)))
	DECODE(TRUE,
		descript IS NULL, 'N/A',
		IS_SPACES(descript), 'N/A',
		LENGTH(descript) = 0, 'N/A',
		LTRIM(RTRIM(descript))) AS o_descript
	FROM SQ_SupMedicalClassCodeTypeStage
),
LKP_SupMedicalClassCodeType AS (
	SELECT
	SupMedicalClassCodeTypeId,
	MedicalClassCodeType,
	Description
	FROM (
		SELECT 
		S.SupMedicalClassCodeTypeId as SupMedicalClassCodeTypeId, 
		S.Description as Description, 
		S.MedicalClassCodeType as MedicalClassCodeType
		
		 FROM 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.SupMedicalClassCodeType S
		
		where 
		S.CurrentSnapshotFlag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY MedicalClassCodeType,Description ORDER BY SupMedicalClassCodeTypeId) = 1
),
EXP_TargetLkp_Detect_Changes AS (
	SELECT
	LKP_SupMedicalClassCodeType.SupMedicalClassCodeTypeId AS Lkp_SupMedicalClassCodeTypeId,
	LKP_SupMedicalClassCodeType.MedicalClassCodeType AS Lkp_MedicalClassCodeType,
	LKP_SupMedicalClassCodeType.Description AS Lkp_Description,
	-- *INF*: iif(isnull(Lkp_SupMedicalClassCodeTypeId), 'NEW',
	-- 
	--   iif(
	--    
	--      ltrim(rtrim(Lkp_MedicalClassCodeType)) != ltrim(rtrim(MedicalClassCodeType))
	-- 
	-- or
	-- 
	--     ltrim(rtrim(Lkp_Description)) != ltrim(rtrim(Description)),
	-- 
	--      'UPDATE', 'NOCHANGE' )
	-- 
	--     )
	IFF(Lkp_SupMedicalClassCodeTypeId IS NULL, 'NEW', IFF(ltrim(rtrim(Lkp_MedicalClassCodeType)) != ltrim(rtrim(MedicalClassCodeType)) OR ltrim(rtrim(Lkp_Description)) != ltrim(rtrim(Description)), 'UPDATE', 'NOCHANGE')) AS v_ChangedFlag,
	v_ChangedFlag AS ChangedFlag,
	EXP_Src_Value.o_code_type AS MedicalClassCodeType,
	EXP_Src_Value.o_descript AS Description,
	-- *INF*: iif(v_ChangedFlag='NEW',
	-- 	to_date('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'),sysdate)
	IFF(v_ChangedFlag = 'NEW', to_date('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'), sysdate) AS EffectiveDate,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS ExpirationDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS SourceSystemId,
	SYSDATE AS CreatedDate,
	SYSDATE AS ModifiedDate,
	1 AS CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditId
	FROM EXP_Src_Value
	LEFT JOIN LKP_SupMedicalClassCodeType
	ON LKP_SupMedicalClassCodeType.MedicalClassCodeType = EXP_Src_Value.o_code_type AND LKP_SupMedicalClassCodeType.Description = EXP_Src_Value.o_descript
),
FIL_Lkp_Records AS (
	SELECT
	ChangedFlag, 
	MedicalClassCodeType, 
	Description, 
	EffectiveDate, 
	ExpirationDate, 
	SourceSystemId, 
	CreatedDate, 
	ModifiedDate, 
	CurrentSnapshotFlag, 
	AuditId
	FROM EXP_TargetLkp_Detect_Changes
	WHERE ChangedFlag = 'NEW' or ChangedFlag = 'UPDATE'
),
SupMedicalClassCodeType_Insert AS (
	INSERT INTO SupMedicalClassCodeType
	(CurrentSnapshotFlag, AuditId, EffectiveDate, ExpirationDate, SourceSystemId, CreatedDate, ModifiedDate, MedicalClassCodeType, Description)
	SELECT 
	CURRENTSNAPSHOTFLAG, 
	AUDITID, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	SOURCESYSTEMID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	MEDICALCLASSCODETYPE, 
	DESCRIPTION
	FROM FIL_Lkp_Records
),
SQ_SupMedicalClassCodeType AS (
	SELECT
	A.SupMedicalClassCodeTypeId,
	A.EffectiveDate,
	A.ExpirationDate,
	A.MedicalClassCodeType 
	
	FROM
	 @{pipeline().parameters.TARGET_TABLE_OWNER}.SupMedicalClassCodeType A
	
	WHERE Exists 
	    (
	SELECT 1
	FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.SupMedicalClassCodeType B
	
	WHERE
	B.CurrentSnapshotFlag = 1
	AND
	A.MedicalClassCodeType = B.MedicalClassCodeType
	
	GROUP BY
	B.MedicalClassCodeType
	
	HAVING 
	COUNT(*) > 1
	       )
	
	AND
	A.CurrentSnapshotFlag = 1
	
	ORDER BY
	A.MedicalClassCodeType,
	A.EffectiveDate DESC
),
EXP_Lag_ExpirationDate AS (
	SELECT
	SupMedicalClassCodeTypeId,
	EffectiveDate,
	ExpirationDate AS orig_ExpirationDate,
	-- *INF*: DECODE(TRUE,
	-- 	MedicalClassCodeType= v_PREV_ROW_MedicalClassCodeType, ADD_TO_DATE(v_PREV_ROW_EffectiveDate,'SS',-1),
	-- 	orig_ExpirationDate)
	DECODE(TRUE,
		MedicalClassCodeType = v_PREV_ROW_MedicalClassCodeType, ADD_TO_DATE(v_PREV_ROW_EffectiveDate, 'SS', - 1),
		orig_ExpirationDate) AS v_ExpirationDate,
	v_ExpirationDate AS ExpirationDate,
	EffectiveDate AS v_PREV_ROW_EffectiveDate,
	MedicalClassCodeType AS v_PREV_ROW_MedicalClassCodeType,
	MedicalClassCodeType,
	SYSDATE AS ModifiedDate,
	0 AS CurrentSnapshotFlag
	FROM SQ_SupMedicalClassCodeType
),
FIL_FirstRowAkId AS (
	SELECT
	SupMedicalClassCodeTypeId, 
	orig_ExpirationDate, 
	ExpirationDate, 
	ModifiedDate, 
	CurrentSnapshotFlag
	FROM EXP_Lag_ExpirationDate
	WHERE orig_ExpirationDate != ExpirationDate
),
UPD_SupMedicalClassCodeType AS (
	SELECT
	SupMedicalClassCodeTypeId, 
	ExpirationDate, 
	ModifiedDate, 
	CurrentSnapshotFlag
	FROM FIL_FirstRowAkId
),
SupMedicalClassCodeType_Update AS (
	MERGE INTO SupMedicalClassCodeType AS T
	USING UPD_SupMedicalClassCodeType AS S
	ON T.SupMedicalClassCodeTypeId = S.SupMedicalClassCodeTypeId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.CurrentSnapshotFlag = S.CurrentSnapshotFlag, T.ExpirationDate = S.ExpirationDate, T.ModifiedDate = S.ModifiedDate
),