WITH
SQ_sup_eor_excl_rsn_stage AS (
	SELECT sup_eor_excl_rsn_stage.sup_eor_excl_rsn_stage_id, sup_eor_excl_rsn_stage.autopay_excl_rsn_code, sup_eor_excl_rsn_stage.description, sup_eor_excl_rsn_stage.exclude_from_manualpay, sup_eor_excl_rsn_stage.expiration_date, sup_eor_excl_rsn_stage.created_user_id, sup_eor_excl_rsn_stage.created_ts, sup_eor_excl_rsn_stage.modified_user_id, sup_eor_excl_rsn_stage.modified_ts, sup_eor_excl_rsn_stage.extract_date, sup_eor_excl_rsn_stage.source_system_id 
	FROM
	 @{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_eor_excl_rsn_stage sup_eor_excl_rsn_stage
),
EXP_arch_sup_eor_excl_rsn_stage AS (
	SELECT
	sup_eor_excl_rsn_stage_id,
	autopay_excl_rsn_code,
	description,
	exclude_from_manualpay,
	expiration_date,
	created_user_id,
	created_ts,
	modified_user_id,
	modified_ts,
	extract_date,
	source_system_id,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AUDIT_ID_OP
	FROM SQ_sup_eor_excl_rsn_stage
),
arch_sup_eor_excl_rsn_stage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.arch_sup_eor_excl_rsn_stage
	(sup_eor_excl_rsn_stage_id, autopay_excl_rsn_code, description, exclude_from_manualpay, expiration_date, created_user_id, created_ts, modified_user_id, modified_ts, extract_date, source_system_id, audit_id)
	SELECT 
	SUP_EOR_EXCL_RSN_STAGE_ID, 
	AUTOPAY_EXCL_RSN_CODE, 
	DESCRIPTION, 
	EXCLUDE_FROM_MANUALPAY, 
	EXPIRATION_DATE, 
	CREATED_USER_ID, 
	CREATED_TS, 
	MODIFIED_USER_ID, 
	MODIFIED_TS, 
	EXTRACT_DATE, 
	SOURCE_SYSTEM_ID, 
	AUDIT_ID_OP AS AUDIT_ID
	FROM EXP_arch_sup_eor_excl_rsn_stage
),