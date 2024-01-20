WITH
SQ_eor_excl_reason_stage AS (
	SELECT eor_excl_reason_stage.eor_excl_reason_stage_id, eor_excl_reason_stage.med_bill_id, eor_excl_reason_stage.autopay_excl_rsn_code, eor_excl_reason_stage.created_user_id, eor_excl_reason_stage.created_ts, eor_excl_reason_stage.modified_user_id, eor_excl_reason_stage.modified_ts, eor_excl_reason_stage.extract_date, eor_excl_reason_stage.source_system_id 
	FROM
	 @{pipeline().parameters.SOURCE_TABLE_OWNER}.eor_excl_reason_stage eor_excl_reason_stage
),
EXP_arch_eor_excl_reason_stage AS (
	SELECT
	eor_excl_reason_stage_id,
	med_bill_id,
	autopay_excl_rsn_code,
	created_user_id,
	created_ts,
	modified_user_id,
	modified_ts,
	extract_date,
	source_system_id,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AUDIT_ID_OP
	FROM SQ_eor_excl_reason_stage
),
arch_eor_excl_reason_stage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.arch_eor_excl_reason_stage
	(eor_excl_rsn_stage_id, med_bill_id, autopay_excl_rsn_code, created_user_id, created_ts, modified_user_id, modified_ts, extract_date, source_system_id, audit_id)
	SELECT 
	eor_excl_reason_stage_id AS EOR_EXCL_RSN_STAGE_ID, 
	MED_BILL_ID, 
	AUTOPAY_EXCL_RSN_CODE, 
	CREATED_USER_ID, 
	CREATED_TS, 
	MODIFIED_USER_ID, 
	MODIFIED_TS, 
	EXTRACT_DATE, 
	SOURCE_SYSTEM_ID, 
	AUDIT_ID_OP AS AUDIT_ID
	FROM EXP_arch_eor_excl_reason_stage
),