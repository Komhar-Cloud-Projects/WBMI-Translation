WITH
SQ_aia56_desc_stage AS (
	SELECT
		aia56_desc_stage_id,
		rec_code,
		description,
		extract_date,
		as_of_date,
		record_count,
		source_system_id
	FROM aia56_desc_stage
),
EXP_AUDIT_FIELDS AS (
	SELECT
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AUDIT_ID_OP,
	aia56_desc_stage_id,
	rec_code,
	description,
	extract_date,
	as_of_date,
	record_count,
	source_system_id
	FROM SQ_aia56_desc_stage
),
arch_aia56_desc_stage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.arch_aia56_desc_stage
	(aia56_desc_stage_id, rec_code, description, extract_date, as_of_date, record_count, source_system_id, audit_id)
	SELECT 
	AIA56_DESC_STAGE_ID, 
	REC_CODE, 
	DESCRIPTION, 
	EXTRACT_DATE, 
	AS_OF_DATE, 
	RECORD_COUNT, 
	SOURCE_SYSTEM_ID, 
	AUDIT_ID_OP AS AUDIT_ID
	FROM EXP_AUDIT_FIELDS
),