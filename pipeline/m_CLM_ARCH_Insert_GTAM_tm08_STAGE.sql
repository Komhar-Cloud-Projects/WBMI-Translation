WITH
SQ_gtam_tm08_stage AS (
	SELECT
		gtam_tm08_stage_id,
		table_fld,
		key_len,
		location,
		master_company_number,
		major_peril,
		data_len,
		coverage_code,
		extract_date,
		as_of_date,
		record_count,
		source_system_id
	FROM gtam_tm08_stage
),
EXP_arch_GTAM_tm08_stage AS (
	SELECT
	gtam_tm08_stage_id AS tm08_stage_ID,
	table_fld AS TABLE_FLD,
	key_len AS KEY_LEN,
	location AS LOCATION,
	master_company_number,
	major_peril,
	data_len AS DATA_LEN,
	coverage_code,
	extract_date AS EXTRACT_DATE,
	as_of_date AS AS_OF_DATE,
	record_count AS RECORD_COUNT,
	source_system_id AS SOURCE_SYSTEM_ID,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AUDIT_ID_OP
	FROM SQ_gtam_tm08_stage
),
arch_gtam_tm08_stage AS (
	INSERT INTO arch_gtam_tm08_stage
	(gtam_tm08_stage_id, table_fld, key_len, location, master_company_number, major_peril, data_len, coverage_code, extract_date, as_of_date, record_count, source_system_id, audit_id)
	SELECT 
	tm08_stage_ID AS GTAM_TM08_STAGE_ID, 
	TABLE_FLD AS TABLE_FLD, 
	KEY_LEN AS KEY_LEN, 
	LOCATION AS LOCATION, 
	MASTER_COMPANY_NUMBER, 
	MAJOR_PERIL, 
	DATA_LEN AS DATA_LEN, 
	COVERAGE_CODE, 
	EXTRACT_DATE AS EXTRACT_DATE, 
	AS_OF_DATE AS AS_OF_DATE, 
	RECORD_COUNT AS RECORD_COUNT, 
	SOURCE_SYSTEM_ID AS SOURCE_SYSTEM_ID, 
	AUDIT_ID_OP AS AUDIT_ID
	FROM EXP_arch_GTAM_tm08_stage
),