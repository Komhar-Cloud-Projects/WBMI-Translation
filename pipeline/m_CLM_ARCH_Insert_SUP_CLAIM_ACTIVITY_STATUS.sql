WITH
SQ_sup_activity_stage AS (
	SELECT
		sup_activity_stage_id,
		act_status_code,
		act_status_desc,
		modified_date,
		modified_user_id,
		extract_date,
		as_of_date,
		record_count,
		source_system_id
	FROM sup_activity_stage
),
EXP_arch_sup_eor_vendor_stage AS (
	SELECT
	sup_activity_stage_id,
	act_status_code,
	act_status_desc,
	modified_date,
	modified_user_id,
	extract_date,
	as_of_date,
	record_count,
	source_system_id,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AUDIT_ID_OP
	FROM SQ_sup_activity_stage
),
arch_sup_activity_stage AS (
	INSERT INTO arch_sup_activity_stage
	(sup_activity_stage_id, act_status_code, act_status_desc, modified_date, modified_user_id, extract_date, as_of_date, record_count, source_system_id, audit_id)
	SELECT 
	SUP_ACTIVITY_STAGE_ID, 
	ACT_STATUS_CODE, 
	ACT_STATUS_DESC, 
	MODIFIED_DATE, 
	MODIFIED_USER_ID, 
	EXTRACT_DATE, 
	AS_OF_DATE, 
	RECORD_COUNT, 
	SOURCE_SYSTEM_ID, 
	AUDIT_ID_OP AS AUDIT_ID
	FROM EXP_arch_sup_eor_vendor_stage
),