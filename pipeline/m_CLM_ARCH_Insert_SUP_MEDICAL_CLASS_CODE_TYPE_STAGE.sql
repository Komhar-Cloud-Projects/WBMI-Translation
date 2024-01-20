WITH
SQ_SupMedicalClassCodeTypeStage AS (
	SELECT 
	S.SupMedicalClassCodeTypeStageId, 
	S.med_class_code_type_id, 
	S.code_type, 
	S.descript, 
	S.created_user_id, 
	S.created_date, 
	S.modified_user_id, 
	S.modified_date, 
	S.ExtractDate, 
	S.SourceSystemId
	 
	FROM
	 SupMedicalClassCodeTypeStage S
),
EXP_Src_Value AS (
	SELECT
	SupMedicalClassCodeTypeStageId,
	med_class_code_type_id,
	code_type,
	-- *INF*: iif(isnull(ltrim(rtrim(code_type))),-1,code_type )
	IFF(ltrim(rtrim(code_type)) IS NULL, - 1, code_type) AS o_code_type,
	descript,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(descript),'N/A',
	-- IS_SPACES(descript),'N/A',
	-- LENGTH(descript)=0,'N/A',
	-- LTRIM(RTRIM(descript)))
	DECODE(
	    TRUE,
	    descript IS NULL, 'N/A',
	    LENGTH(descript)>0 AND TRIM(descript)='', 'N/A',
	    LENGTH(descript) = 0, 'N/A',
	    LTRIM(RTRIM(descript))
	) AS o_descript,
	created_user_id,
	created_date,
	modified_user_id,
	modified_date,
	ExtractDate,
	SourceSystemId
	FROM SQ_SupMedicalClassCodeTypeStage
),
LKP_ArchSupMedicalClassCodeType_Target AS (
	SELECT
	ArchSupMedicalClassCodeTypeStageId,
	SupMedicalClassCodeTypeStageId,
	med_class_code_type_id,
	code_type,
	descript
	FROM (
		SELECT 
			ArchSupMedicalClassCodeTypeStageId,
			SupMedicalClassCodeTypeStageId,
			med_class_code_type_id,
			code_type,
			descript
		FROM ArchSupMedicalClassCodeTypeStage
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY SupMedicalClassCodeTypeStageId ORDER BY ArchSupMedicalClassCodeTypeStageId) = 1
),
EXP_ArchSupMedicalClassCodeStage AS (
	SELECT
	LKP_ArchSupMedicalClassCodeType_Target.ArchSupMedicalClassCodeTypeStageId AS Lkp_ArchSupMedicalClassCodeTypeStageId,
	LKP_ArchSupMedicalClassCodeType_Target.SupMedicalClassCodeTypeStageId AS Lkp_SupMedicalClassCodeTypeStageId1,
	LKP_ArchSupMedicalClassCodeType_Target.med_class_code_type_id AS Lkp_med_class_code_type_id,
	LKP_ArchSupMedicalClassCodeType_Target.code_type AS Lkp_code_type,
	LKP_ArchSupMedicalClassCodeType_Target.descript AS Lkp_descript,
	-- *INF*: iif(isnull(Lkp_ArchSupMedicalClassCodeTypeStageId),'NEW',
	-- 
	-- iif(
	-- 
	-- Lkp_SupMedicalClassCodeTypeStageId1 != SupMedicalClassCodeTypeStageId
	-- 
	-- or 
	-- 
	-- Lkp_med_class_code_type_id != med_class_code_type_id
	-- 
	-- or
	-- 
	-- Lkp_code_type != code_type
	-- 
	-- or
	-- 
	-- ltrim(rtrim(Lkp_descript)) != ltrim(rtrim(descript)),
	-- 
	--       'UPDATE','NOCHANGE' )
	-- )
	IFF(
	    Lkp_ArchSupMedicalClassCodeTypeStageId IS NULL, 'NEW',
	    IFF(
	        Lkp_SupMedicalClassCodeTypeStageId1 != SupMedicalClassCodeTypeStageId
	        or Lkp_med_class_code_type_id != med_class_code_type_id
	        or Lkp_code_type != code_type
	        or ltrim(rtrim(Lkp_descript)) != ltrim(rtrim(descript)),
	        'UPDATE',
	        'NOCHANGE'
	    )
	) AS v_ChangedFlag,
	v_ChangedFlag AS ChangedFlag,
	EXP_Src_Value.SupMedicalClassCodeTypeStageId,
	EXP_Src_Value.med_class_code_type_id,
	EXP_Src_Value.o_code_type AS code_type,
	EXP_Src_Value.o_descript AS descript,
	-- *INF*: ltrim(rtrim(descript))
	ltrim(rtrim(descript)) AS o_descript,
	EXP_Src_Value.created_user_id,
	EXP_Src_Value.created_date,
	EXP_Src_Value.modified_user_id,
	EXP_Src_Value.modified_date,
	EXP_Src_Value.ExtractDate,
	EXP_Src_Value.SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM EXP_Src_Value
	LEFT JOIN LKP_ArchSupMedicalClassCodeType_Target
	ON LKP_ArchSupMedicalClassCodeType_Target.SupMedicalClassCodeTypeStageId = EXP_Src_Value.SupMedicalClassCodeTypeStageId
),
FIL_ArchSupMedicalClassCodeTypeStage AS (
	SELECT
	ChangedFlag, 
	SupMedicalClassCodeTypeStageId, 
	med_class_code_type_id, 
	code_type, 
	o_descript, 
	created_user_id, 
	created_date, 
	modified_user_id, 
	modified_date, 
	ExtractDate, 
	SourceSystemId, 
	o_AuditId
	FROM EXP_ArchSupMedicalClassCodeStage
	WHERE ChangedFlag = 'NEW' or ChangedFlag = 'UPDATE'
),
ArchSupMedicalClassCodeTypeStage AS (
	INSERT INTO ArchSupMedicalClassCodeTypeStage
	(SupMedicalClassCodeTypeStageId, med_class_code_type_id, code_type, descript, created_user_id, created_date, modified_user_id, modified_date, ExtractDate, SourceSystemId, AuditId)
	SELECT 
	SUPMEDICALCLASSCODETYPESTAGEID, 
	MED_CLASS_CODE_TYPE_ID, 
	CODE_TYPE, 
	o_descript AS DESCRIPT, 
	CREATED_USER_ID, 
	CREATED_DATE, 
	MODIFIED_USER_ID, 
	MODIFIED_DATE, 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID
	FROM FIL_ArchSupMedicalClassCodeTypeStage
),