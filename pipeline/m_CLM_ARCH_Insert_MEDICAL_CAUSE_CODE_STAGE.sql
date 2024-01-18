WITH
SQ_MedicalCauseCodeStage AS (
	SELECT 
	M.MedicalCauseCodeStageId, 
	M.code, 
	M.short_descript, 
	M.long_descript, 
	M.med_class_code_type_id, 
	M.ExtractDate, 
	M.SourceSystemId
	 
	FROM
	 MedicalCauseCodeStage M
),
EXP_Src_Value AS (
	SELECT
	code,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(code),'N/A',
	-- IS_SPACES(code),'N/A',
	-- LENGTH(code)=0,'N/A',
	-- LTRIM(RTRIM(code)))
	DECODE(
	    TRUE,
	    code IS NULL, 'N/A',
	    LENGTH(code)>0 AND TRIM(code)='', 'N/A',
	    LENGTH(code) = 0, 'N/A',
	    LTRIM(RTRIM(code))
	) AS o_code,
	short_descript,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(short_descript),'N/A',
	-- IS_SPACES(short_descript),'N/A',
	-- LENGTH(short_descript)=0,'N/A',
	-- LTRIM(RTRIM(short_descript)))
	DECODE(
	    TRUE,
	    short_descript IS NULL, 'N/A',
	    LENGTH(short_descript)>0 AND TRIM(short_descript)='', 'N/A',
	    LENGTH(short_descript) = 0, 'N/A',
	    LTRIM(RTRIM(short_descript))
	) AS o_short_descript,
	long_descript,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(long_descript),'N/A',
	-- IS_SPACES(long_descript),'N/A',
	-- LENGTH(long_descript)=0,'N/A',
	-- LTRIM(RTRIM(long_descript)))
	DECODE(
	    TRUE,
	    long_descript IS NULL, 'N/A',
	    LENGTH(long_descript)>0 AND TRIM(long_descript)='', 'N/A',
	    LENGTH(long_descript) = 0, 'N/A',
	    LTRIM(RTRIM(long_descript))
	) AS o_long_descript,
	med_class_code_type_id,
	ExtractDate,
	SourceSystemId
	FROM SQ_MedicalCauseCodeStage
),
LKP_ArchMedicalCauseCode_Target AS (
	SELECT
	ArchMedicalCauseCodeStageId,
	MedicalCauseCodeStageId,
	code,
	med_class_code_type_id,
	short_descript,
	long_descript
	FROM (
		SELECT 
			ArchMedicalCauseCodeStageId,
			MedicalCauseCodeStageId,
			code,
			med_class_code_type_id,
			short_descript,
			long_descript
		FROM ArchMedicalCauseCodeStage
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY MedicalCauseCodeStageId ORDER BY ArchMedicalCauseCodeStageId) = 1
),
EXP_ArchMedicalCauseCodeStage AS (
	SELECT
	LKP_ArchMedicalCauseCode_Target.ArchMedicalCauseCodeStageId AS Lkp_ArchMedicalCauseCodeStageId,
	LKP_ArchMedicalCauseCode_Target.MedicalCauseCodeStageId AS Lkp_MedicalCauseCodeStageId,
	LKP_ArchMedicalCauseCode_Target.code AS Lkp_code,
	LKP_ArchMedicalCauseCode_Target.med_class_code_type_id AS Lkp_med_class_code_type_id,
	LKP_ArchMedicalCauseCode_Target.short_descript AS Lkp_short_descript,
	LKP_ArchMedicalCauseCode_Target.long_descript AS Lkp_long_descript,
	SQ_MedicalCauseCodeStage.MedicalCauseCodeStageId,
	EXP_Src_Value.o_code AS code,
	-- *INF*: ltrim(rtrim(code))
	ltrim(rtrim(code)) AS o_code,
	EXP_Src_Value.o_short_descript AS short_descript,
	-- *INF*: ltrim(rtrim(short_descript))
	ltrim(rtrim(short_descript)) AS o_short_descript,
	EXP_Src_Value.o_long_descript AS long_descript,
	-- *INF*: ltrim(rtrim(long_descript))
	ltrim(rtrim(long_descript)) AS o_long_descript,
	EXP_Src_Value.med_class_code_type_id,
	v_ChangedFlag AS ChangedFlag,
	-- *INF*: iif(isnull(Lkp_ArchMedicalCauseCodeStageId),'NEW',
	-- 
	--         iif(
	-- 
	--         LTRIM(RTRIM(code)) != LTRIM(RTRIM(Lkp_code)) 
	-- 
	-- or
	--   
	--         LTRIM(RTRIM(med_class_code_type_id)) != LTRIM(RTRIM(Lkp_med_class_code_type_id))
	-- 
	-- or
	--   
	--         LTRIM(RTRIM(short_descript)) != LTRIM(RTRIM(Lkp_short_descript))
	-- 
	-- or
	--   
	--         LTRIM(RTRIM(Lkp_MedicalCauseCodeStageId)) != LTRIM(RTRIM(MedicalCauseCodeStageId))
	-- 
	-- or
	--   
	--         LTRIM(RTRIM(long_descript)) != LTRIM(RTRIM(Lkp_long_descript)),
	-- 
	--         
	--         'UPDATE', 'NOCHANGE')
	-- 
	--    )
	IFF(
	    Lkp_ArchMedicalCauseCodeStageId IS NULL, 'NEW',
	    IFF(
	        LTRIM(RTRIM(code)) != LTRIM(RTRIM(Lkp_code))
	        or LTRIM(RTRIM(med_class_code_type_id)) != LTRIM(RTRIM(Lkp_med_class_code_type_id))
	        or LTRIM(RTRIM(short_descript)) != LTRIM(RTRIM(Lkp_short_descript))
	        or LTRIM(RTRIM(Lkp_MedicalCauseCodeStageId)) != LTRIM(RTRIM(MedicalCauseCodeStageId))
	        or LTRIM(RTRIM(long_descript)) != LTRIM(RTRIM(Lkp_long_descript)),
	        'UPDATE',
	        'NOCHANGE'
	    )
	) AS v_ChangedFlag,
	EXP_Src_Value.ExtractDate,
	EXP_Src_Value.SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM EXP_Src_Value
	 -- Manually join with SQ_MedicalCauseCodeStage
	LEFT JOIN LKP_ArchMedicalCauseCode_Target
	ON LKP_ArchMedicalCauseCode_Target.MedicalCauseCodeStageId = SQ_MedicalCauseCodeStage.MedicalCauseCodeStageId
),
FIL_ArchMedicalCauseCodeStage AS (
	SELECT
	MedicalCauseCodeStageId, 
	o_code, 
	o_short_descript, 
	o_long_descript, 
	med_class_code_type_id, 
	ChangedFlag, 
	ExtractDate, 
	SourceSystemId, 
	o_AuditId
	FROM EXP_ArchMedicalCauseCodeStage
	WHERE ChangedFlag = 'NEW' or ChangedFlag = 'UPDATE'
),
ArchMedicalCauseCodeStage AS (
	INSERT INTO ArchMedicalCauseCodeStage
	(MedicalCauseCodeStageId, code, short_descript, long_descript, med_class_code_type_id, ExtractDate, SourceSystemId, AuditId)
	SELECT 
	MEDICALCAUSECODESTAGEID, 
	o_code AS CODE, 
	o_short_descript AS SHORT_DESCRIPT, 
	o_long_descript AS LONG_DESCRIPT, 
	MED_CLASS_CODE_TYPE_ID, 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID
	FROM FIL_ArchMedicalCauseCodeStage
),