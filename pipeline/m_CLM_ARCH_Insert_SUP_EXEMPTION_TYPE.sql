WITH
SQ_sup_exemption_type_stage AS (
	SELECT
		sup_exemption_type_stage_id,
		exemption_type_code,
		exemption_type_desc,
		modified_date,
		modified_user_id,
		extract_date,
		as_of_date,
		record_count,
		source_system_id
	FROM sup_exemption_type_stage
),
EXP_AUDIT_FIELDS AS (
	SELECT
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AUDIT_ID_OP,
	sup_exemption_type_stage_id,
	exemption_type_code,
	exemption_type_desc,
	modified_date,
	modified_user_id,
	extract_date,
	as_of_date,
	record_count,
	source_system_id
	FROM SQ_sup_exemption_type_stage
),
arch_sup_exemption_type_stage AS (
	INSERT INTO arch_sup_exemption_type_stage
	(sup_exemption_type_stage_id, exemption_type_code, exemption_type_desc, modified_date, modified_user_id, extract_date, as_of_date, record_count, source_system_id, audit_id)
	SELECT 
	SUP_EXEMPTION_TYPE_STAGE_ID, 
	EXEMPTION_TYPE_CODE, 
	EXEMPTION_TYPE_DESC, 
	MODIFIED_DATE, 
	MODIFIED_USER_ID, 
	EXTRACT_DATE, 
	AS_OF_DATE, 
	RECORD_COUNT, 
	SOURCE_SYSTEM_ID, 
	AUDIT_ID_OP AS AUDIT_ID
	FROM EXP_AUDIT_FIELDS
),