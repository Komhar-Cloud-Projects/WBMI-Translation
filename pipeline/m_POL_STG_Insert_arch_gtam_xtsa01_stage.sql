WITH
SQ_gtam_xtsa01_stage1 AS (
	SELECT
		gtam_xtsa01_stage_id,
		table_fld,
		key_len,
		field_label,
		code,
		data_len,
		major_peril_translation,
		extract_date,
		as_of_date,
		record_count,
		source_system_id
	FROM gtam_xtsa01_stage
),
LKP_arch_gtam_xtsa01_stage AS (
	SELECT
	arch_gtam_xtsa01_stage_id,
	major_peril_translation,
	field_label,
	code
	FROM (
		SELECT tl.arch_gtam_xtsa01_stage_id as arch_gtam_xtsa01_stage_id, 
		tl.major_peril_translation as major_peril_translation,
		 tl.field_label as field_label,
		  tl.code as code
		FROM arch_gtam_xtsa01_stage tl
		where 	tl.arch_gtam_xtsa01_stage_id In
			(Select max(arch_gtam_xtsa01_stage_id) from arch_gtam_xtsa01_stage b
			group by b.field_label, b.code)
		order by tl.field_label, tl.code--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY field_label,code ORDER BY arch_gtam_xtsa01_stage_id) = 1
),
EXP_arch_xtsa01_stage AS (
	SELECT
	SQ_gtam_xtsa01_stage1.gtam_xtsa01_stage_id,
	SQ_gtam_xtsa01_stage1.table_fld,
	SQ_gtam_xtsa01_stage1.key_len,
	SQ_gtam_xtsa01_stage1.field_label,
	SQ_gtam_xtsa01_stage1.code,
	SQ_gtam_xtsa01_stage1.data_len,
	SQ_gtam_xtsa01_stage1.major_peril_translation,
	SQ_gtam_xtsa01_stage1.extract_date AS EXTRACT_DATE,
	SQ_gtam_xtsa01_stage1.as_of_date AS AS_OF_DATE,
	SQ_gtam_xtsa01_stage1.record_count AS RECORD_COUNT,
	SQ_gtam_xtsa01_stage1.source_system_id AS SOURCE_SYSTEM_ID,
	LKP_arch_gtam_xtsa01_stage.arch_gtam_xtsa01_stage_id AS LKP_arch_gtam_xtsa01_stage_id,
	LKP_arch_gtam_xtsa01_stage.major_peril_translation AS LKP_major_peril_translation,
	-- *INF*: iif(isnull(LKP_arch_gtam_xtsa01_stage_id),'NEW',
	--     iif((
	-- ltrim(rtrim(LKP_major_peril_translation )) <> ltrim(rtrim(major_peril_translation))), 'UPDATE', 'NOCHANGE'))
	IFF(
	    LKP_arch_gtam_xtsa01_stage_id IS NULL, 'NEW',
	    IFF(
	        (ltrim(rtrim(LKP_major_peril_translation)) <> ltrim(rtrim(major_peril_translation))),
	        'UPDATE',
	        'NOCHANGE'
	    )
	) AS v_Changed_Flag,
	v_Changed_Flag AS Changed_Flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AUDIT_ID
	FROM SQ_gtam_xtsa01_stage1
	LEFT JOIN LKP_arch_gtam_xtsa01_stage
	ON LKP_arch_gtam_xtsa01_stage.field_label = SQ_gtam_xtsa01_stage1.field_label AND LKP_arch_gtam_xtsa01_stage.code = SQ_gtam_xtsa01_stage1.code
),
FIL_Inserts AS (
	SELECT
	gtam_xtsa01_stage_id, 
	table_fld, 
	key_len, 
	field_label, 
	code, 
	data_len, 
	major_peril_translation, 
	EXTRACT_DATE, 
	AS_OF_DATE, 
	RECORD_COUNT, 
	SOURCE_SYSTEM_ID, 
	Changed_Flag, 
	AUDIT_ID
	FROM EXP_arch_xtsa01_stage
	WHERE Changed_Flag = 'NEW' or Changed_Flag = 'UPDATE'
),
arch_gtam_xtsa01_stage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.arch_gtam_xtsa01_stage
	(gtam_xtsa01_stage_id, table_fld, key_len, field_label, code, data_len, major_peril_translation, extract_date, as_of_date, record_count, source_system_id, audit_id)
	SELECT 
	GTAM_XTSA01_STAGE_ID, 
	TABLE_FLD, 
	KEY_LEN, 
	FIELD_LABEL, 
	CODE, 
	DATA_LEN, 
	MAJOR_PERIL_TRANSLATION, 
	EXTRACT_DATE AS EXTRACT_DATE, 
	AS_OF_DATE AS AS_OF_DATE, 
	RECORD_COUNT AS RECORD_COUNT, 
	SOURCE_SYSTEM_ID AS SOURCE_SYSTEM_ID, 
	AUDIT_ID AS AUDIT_ID
	FROM FIL_Inserts
),