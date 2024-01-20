WITH
SQ_sup_eor_vendor_stage AS (
	SELECT
		sup_eor_vendor_stage_id,
		sup_vendor_id,
		vendor_code,
		created_date,
		created_user_id,
		modified_date,
		modified_user_id,
		vendor_name,
		vendor_addr,
		vendor_city,
		vendor_state,
		vendor_zip,
		vendor_ph,
		vendor_fax,
		vendor_disclaimer,
		extract_date,
		as_of_date,
		rcrd_count,
		source_sys_id
	FROM sup_eor_vendor_stage
),
EXP_arch_sup_eor_vendor_stage AS (
	SELECT
	sup_eor_vendor_stage_id,
	sup_vendor_id,
	vendor_code,
	created_date,
	created_user_id,
	modified_date,
	modified_user_id,
	vendor_name,
	vendor_addr,
	vendor_city,
	vendor_state,
	vendor_zip,
	vendor_ph,
	vendor_fax,
	vendor_disclaimer,
	extract_date,
	as_of_date,
	rcrd_count,
	source_sys_id,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id
	FROM SQ_sup_eor_vendor_stage
),
arch_sup_eor_vendor_stage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.arch_sup_eor_vendor_stage
	(sup_eor_vendor_stage_id, sup_vendor_id, vendor_code, created_date, created_user_id, modified_date, modified_user_id, vendor_name, vendor_addr, vendor_city, vendor_state, vendor_zip, vendor_ph, vendor_fax, vendor_disclaimer, extract_date, as_of_date, rcrd_count, source_sys_id, audit_id)
	SELECT 
	SUP_EOR_VENDOR_STAGE_ID, 
	SUP_VENDOR_ID, 
	VENDOR_CODE, 
	CREATED_DATE, 
	CREATED_USER_ID, 
	MODIFIED_DATE, 
	MODIFIED_USER_ID, 
	VENDOR_NAME, 
	VENDOR_ADDR, 
	VENDOR_CITY, 
	VENDOR_STATE, 
	VENDOR_ZIP, 
	VENDOR_PH, 
	VENDOR_FAX, 
	VENDOR_DISCLAIMER, 
	EXTRACT_DATE, 
	AS_OF_DATE, 
	RCRD_COUNT, 
	SOURCE_SYS_ID, 
	AUDIT_ID
	FROM EXP_arch_sup_eor_vendor_stage
),