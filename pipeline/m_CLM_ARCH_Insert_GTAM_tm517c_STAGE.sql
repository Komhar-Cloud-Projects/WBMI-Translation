WITH
SQ_gtam_tm517c_stage AS (
	SELECT
		tm517c_stage_id,
		table_fld,
		key_len,
		line_of_business,
		insurance_line,
		risk_unit_group,
		product_type_code,
		language_indicator,
		data_len,
		risk_unit_group_literal,
		extract_date,
		as_of_date,
		record_count,
		source_sytem_id
	FROM gtam_tm517c_stage
),
EXP_arch_gtam_tm517c_stage AS (
	SELECT
	tm517c_stage_id,
	table_fld,
	key_len,
	line_of_business,
	insurance_line,
	risk_unit_group,
	product_type_code,
	language_indicator,
	data_len,
	risk_unit_group_literal,
	extract_date,
	as_of_date,
	record_count,
	source_sytem_id,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AUDIT_ID_OP
	FROM SQ_gtam_tm517c_stage
),
arch_gtam_tm517c_stage AS (
	INSERT INTO arch_gtam_tm517c_stage
	(tm517c_stage_id, table_fld, key_len, line_of_business, insurance_line, risk_unit_group, product_type_code, language_indicator, data_len, risk_unit_group_literal, extract_date, as_of_date, record_count, source_sytem_id, audit_id)
	SELECT 
	TM517C_STAGE_ID, 
	TABLE_FLD, 
	KEY_LEN, 
	LINE_OF_BUSINESS, 
	INSURANCE_LINE, 
	RISK_UNIT_GROUP, 
	PRODUCT_TYPE_CODE, 
	LANGUAGE_INDICATOR, 
	DATA_LEN, 
	RISK_UNIT_GROUP_LITERAL, 
	EXTRACT_DATE, 
	AS_OF_DATE, 
	RECORD_COUNT, 
	SOURCE_SYTEM_ID, 
	AUDIT_ID_OP AS AUDIT_ID
	FROM EXP_arch_gtam_tm517c_stage
),