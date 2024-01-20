WITH
SQ_gtam_tc08_stage AS (
	SELECT
		tc08_stage_id,
		table_fld,
		key_len,
		code_entered_on_pucl,
		data_len,
		payee_phrase_verbiage,
		extract_date,
		as_of_date,
		record_count,
		source_system_id
	FROM gtam_tc08_stage
),
EXP_arch_GTAM_tc26_stage AS (
	SELECT
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AUDIT_ID_OP,
	tc08_stage_id,
	table_fld,
	key_len,
	code_entered_on_pucl,
	data_len,
	payee_phrase_verbiage,
	extract_date,
	as_of_date,
	record_count,
	source_system_id
	FROM SQ_gtam_tc08_stage
),
arch_gtam_tc08_stage AS (
	INSERT INTO arch_gtam_tc08_stage
	(tc08_stage_id, table_fld, key_len, code_entered_on_pucl, data_len, payee_phrase_verbiage, extract_date, as_of_date, record_count, source_system_id, audit_id)
	SELECT 
	TC08_STAGE_ID, 
	TABLE_FLD, 
	KEY_LEN, 
	CODE_ENTERED_ON_PUCL, 
	DATA_LEN, 
	PAYEE_PHRASE_VERBIAGE, 
	EXTRACT_DATE, 
	AS_OF_DATE, 
	RECORD_COUNT, 
	SOURCE_SYSTEM_ID, 
	AUDIT_ID_OP AS AUDIT_ID
	FROM EXP_arch_GTAM_tc26_stage
),