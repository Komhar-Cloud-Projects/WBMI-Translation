WITH
SQ_gtam_tc09_stage AS (
	SELECT
		tc09_stage_id,
		table_fld,
		key_len,
		memo_phrase_on_pucl,
		data_len,
		memo_phrase_verbiage,
		extract_date,
		as_of_date,
		record_count,
		source_system_id
	FROM gtam_tc09_stage
),
EXP_arch_GTAM_tc26_stage AS (
	SELECT
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AUDIT_ID_OP,
	tc09_stage_id,
	table_fld,
	key_len,
	memo_phrase_on_pucl,
	data_len,
	memo_phrase_verbiage,
	extract_date,
	as_of_date,
	record_count,
	source_system_id
	FROM SQ_gtam_tc09_stage
),
arch_gtam_tc09_stage AS (
	INSERT INTO arch_gtam_tc09_stage
	(tc09_stage_id, table_fld, key_len, memo_phrase_on_pucl, data_len, memo_phrase_verbiage, extract_date, as_of_date, record_count, source_system_id, audit_id)
	SELECT 
	TC09_STAGE_ID, 
	TABLE_FLD, 
	KEY_LEN, 
	MEMO_PHRASE_ON_PUCL, 
	DATA_LEN, 
	MEMO_PHRASE_VERBIAGE, 
	EXTRACT_DATE, 
	AS_OF_DATE, 
	RECORD_COUNT, 
	SOURCE_SYSTEM_ID, 
	AUDIT_ID_OP AS AUDIT_ID
	FROM EXP_arch_GTAM_tc26_stage
),