WITH
SQ_vendor_1099_type_stage AS (
	SELECT
	 vendor_1099_type_stage.vendor_1099_type_id, 
	vendor_1099_type_stage.vendor_type_code, 
	vendor_1099_type_stage.vendor_type_desc 
	FROM
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.vendor_1099_type_stage
),
EXP_arch_vendor_1099_stage AS (
	SELECT
	vendor_1099_type_id,
	vendor_type_code,
	vendor_type_desc,
	SYSDATE AS extract_date,
	SYSDATE AS as_of_date,
	'' AS record_count,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS source_system_id,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id
	FROM SQ_vendor_1099_type_stage
),
TGT_arch_vendor_1099_type_stage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.arch_vendor_1099_type_stage
	(vendor_1099_type_stage_id, vendor_type_code, vendor_type_desc, extract_date, as_of_date, record_count, source_system_id, audit_id)
	SELECT 
	vendor_1099_type_id AS VENDOR_1099_TYPE_STAGE_ID, 
	VENDOR_TYPE_CODE, 
	VENDOR_TYPE_DESC, 
	EXTRACT_DATE, 
	AS_OF_DATE, 
	RECORD_COUNT, 
	SOURCE_SYSTEM_ID, 
	AUDIT_ID
	FROM EXP_arch_vendor_1099_stage
),