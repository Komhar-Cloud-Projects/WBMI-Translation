WITH
SQ_gtam_tm04_stage AS (
	SELECT
		gtam_tm04_stage_id,
		table_fld,
		key_len,
		location_code,
		master_company_number,
		branch,
		data_len,
		lm_region,
		extract_date,
		as_of_date,
		record_count,
		source_system_id
	FROM gtam_tm04_stage
),
EXP_arch_GTAM_tm04_stage AS (
	SELECT
	gtam_tm04_stage_id AS tm04_stage_ID,
	table_fld AS TABLE_FLD,
	key_len AS KEY_LEN,
	location_code AS LOCATION_CODE,
	master_company_number,
	branch,
	data_len AS DATA_LEN,
	lm_region,
	extract_date AS EXTRACT_DATE,
	as_of_date AS AS_OF_DATE,
	record_count AS RECORD_COUNT,
	source_system_id AS SOURCE_SYSTEM_ID,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AUDIT_ID_OP
	FROM SQ_gtam_tm04_stage
),
arch_gtam_tm04_stage AS (
	INSERT INTO arch_gtam_tm04_stage
	(gtam_tm04_stage_id, table_fld, key_len, location_code, master_company_number, branch, data_len, lm_region, extract_date, as_of_date, record_count, source_system_id, audit_id)
	SELECT 
	tm04_stage_ID AS GTAM_TM04_STAGE_ID, 
	TABLE_FLD AS TABLE_FLD, 
	KEY_LEN AS KEY_LEN, 
	LOCATION_CODE AS LOCATION_CODE, 
	MASTER_COMPANY_NUMBER, 
	BRANCH, 
	DATA_LEN AS DATA_LEN, 
	LM_REGION, 
	EXTRACT_DATE AS EXTRACT_DATE, 
	AS_OF_DATE AS AS_OF_DATE, 
	RECORD_COUNT AS RECORD_COUNT, 
	SOURCE_SYSTEM_ID AS SOURCE_SYSTEM_ID, 
	AUDIT_ID_OP AS AUDIT_ID
	FROM EXP_arch_GTAM_tm04_stage
),