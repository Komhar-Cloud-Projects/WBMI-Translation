WITH
SQ_MedicalDiagnosisCodeStage AS (
	SELECT 
	M.MedicalDiagnosisCodeStageId, 
	M.code, 
	M.short_descript, 
	M.long_descript, 
	M.med_class_code_type_id, 
	M.ExtractDate, 
	M.SourceSystemId
	 
	FROM
	 MedicalDiagnosisCodeStage M
),
EXP_Src_Value AS (
	SELECT
	MedicalDiagnosisCodeStageId,
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
	FROM SQ_MedicalDiagnosisCodeStage
),
LKP_ArchMedicalDiagnosisCodeStage_Target AS (
	SELECT
	ArchMedicalDiagnosisCodeStageId,
	MedicalDiagnosisCodeStageId,
	code,
	short_descript,
	long_descript,
	med_class_code_type_id
	FROM (
		SELECT 
			ArchMedicalDiagnosisCodeStageId,
			MedicalDiagnosisCodeStageId,
			code,
			short_descript,
			long_descript,
			med_class_code_type_id
		FROM ArchMedicalDiagnosisCodeStage
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY MedicalDiagnosisCodeStageId ORDER BY ArchMedicalDiagnosisCodeStageId) = 1
),
EXP_MedicalDiagnosisCodeStage AS (
	SELECT
	LKP_ArchMedicalDiagnosisCodeStage_Target.ArchMedicalDiagnosisCodeStageId AS Lkp_ArchMedicalDiagnosisCodeStageId,
	LKP_ArchMedicalDiagnosisCodeStage_Target.MedicalDiagnosisCodeStageId AS Lkp_MedicalDiagnosisCodeStageId,
	LKP_ArchMedicalDiagnosisCodeStage_Target.code AS Lkp_code,
	LKP_ArchMedicalDiagnosisCodeStage_Target.short_descript AS Lkp_short_descript,
	LKP_ArchMedicalDiagnosisCodeStage_Target.long_descript AS Lkp_long_descript,
	EXP_Src_Value.MedicalDiagnosisCodeStageId,
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
	-- *INF*: iif(isnull(Lkp_ArchMedicalDiagnosisCodeStageId), 'NEW',
	-- 
	--    iif(
	-- 
	--    ltrim(rtrim(Lkp_code)) != ltrim(rtrim(code))
	-- 
	-- or
	-- 
	--     ltrim(rtrim(Lkp_short_descript)) != ltrim(rtrim(short_descript))
	-- 
	-- or
	-- 
	--     ltrim(rtrim(Lkp_long_descript)) != ltrim(rtrim(long_descript))
	-- 
	-- or
	-- 
	--     ltrim(rtrim(Lkp_MedicalDiagnosisCodeStageId)) != ltrim(rtrim(MedicalDiagnosisCodeStageId)),
	-- 
	--    'UPDATE', 'NOCHANGE')
	-- 
	--    )
	IFF(
	    Lkp_ArchMedicalDiagnosisCodeStageId IS NULL, 'NEW',
	    IFF(
	        ltrim(rtrim(Lkp_code)) != ltrim(rtrim(code))
	        or ltrim(rtrim(Lkp_short_descript)) != ltrim(rtrim(short_descript))
	        or ltrim(rtrim(Lkp_long_descript)) != ltrim(rtrim(long_descript))
	        or ltrim(rtrim(Lkp_MedicalDiagnosisCodeStageId)) != ltrim(rtrim(MedicalDiagnosisCodeStageId)),
	        'UPDATE',
	        'NOCHANGE'
	    )
	) AS v_Changed_Flag,
	v_Changed_Flag AS Changed_Flag,
	EXP_Src_Value.ExtractDate,
	EXP_Src_Value.SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM EXP_Src_Value
	LEFT JOIN LKP_ArchMedicalDiagnosisCodeStage_Target
	ON LKP_ArchMedicalDiagnosisCodeStage_Target.MedicalDiagnosisCodeStageId = EXP_Src_Value.MedicalDiagnosisCodeStageId
),
FIL_ArchMedicalDiagnosisCodeStage AS (
	SELECT
	Changed_Flag, 
	MedicalDiagnosisCodeStageId, 
	o_code, 
	o_short_descript, 
	o_long_descript, 
	med_class_code_type_id, 
	ExtractDate, 
	SourceSystemId, 
	o_AuditId
	FROM EXP_MedicalDiagnosisCodeStage
	WHERE Changed_Flag = 'NEW' or Changed_Flag = 'UPDATE'
),
ArchMedicalDiagnosisCodeStage AS (
	INSERT INTO ArchMedicalDiagnosisCodeStage
	(MedicalDiagnosisCodeStageId, code, short_descript, long_descript, med_class_code_type_id, ExtractDate, SourceSystemId, AuditId)
	SELECT 
	MEDICALDIAGNOSISCODESTAGEID, 
	o_code AS CODE, 
	o_short_descript AS SHORT_DESCRIPT, 
	o_long_descript AS LONG_DESCRIPT, 
	MED_CLASS_CODE_TYPE_ID, 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID
	FROM FIL_ArchMedicalDiagnosisCodeStage
),