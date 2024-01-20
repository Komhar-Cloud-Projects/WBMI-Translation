WITH
SQ_sup_return_type_stage AS (
	SELECT
		sup_return_type_stage_id,
		return_code,
		return_desc,
		modified_date,
		modified_user_id,
		extract_date,
		as_of_date,
		record_count,
		source_system_id
	FROM sup_return_type_stage
),
EXP_AUDIT_FIELDS AS (
	SELECT
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AUDIT_ID_OP,
	sup_return_type_stage_id,
	return_code,
	return_desc,
	modified_date,
	modified_user_id,
	extract_date,
	as_of_date,
	record_count,
	source_system_id
	FROM SQ_sup_return_type_stage
),
arch_sup_return_type_stage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.arch_sup_return_type_stage
	(sup_return_type_stage_id, return_code, return_desc, modified_date, modified_user_id, extract_date, as_of_date, record_count, source_system_id, audit_id)
	SELECT 
	SUP_RETURN_TYPE_STAGE_ID, 
	RETURN_CODE, 
	RETURN_DESC, 
	MODIFIED_DATE, 
	MODIFIED_USER_ID, 
	EXTRACT_DATE, 
	AS_OF_DATE, 
	RECORD_COUNT, 
	SOURCE_SYSTEM_ID, 
	AUDIT_ID_OP AS AUDIT_ID
	FROM EXP_AUDIT_FIELDS
),