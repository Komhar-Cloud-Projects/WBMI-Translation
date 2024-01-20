WITH
SQ_sup_claim_category_stage AS (
	SELECT
		sup_claim_category_stage_id,
		clm_category_code,
		clm_category_desc,
		modified_date,
		modified_user_id,
		extract_date,
		as_of_date,
		record_count,
		source_system_id
	FROM sup_claim_category_stage
),
EXP_arch_sup_claim_category_stage AS (
	SELECT
	sup_claim_category_stage_id,
	clm_category_code,
	clm_category_desc,
	modified_date,
	modified_user_id,
	extract_date,
	as_of_date,
	record_count,
	source_system_id,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AUDIT_ID_OP
	FROM SQ_sup_claim_category_stage
),
arch_sup_claim_category_stage AS (
	INSERT INTO arch_sup_claim_category_stage
	(sup_claim_category_stage_id, clm_category_code, clm_category_desc, modified_date, modified_user_id, extract_date, as_of_date, record_count, source_system_id, audit_id)
	SELECT 
	SUP_CLAIM_CATEGORY_STAGE_ID, 
	CLM_CATEGORY_CODE, 
	CLM_CATEGORY_DESC, 
	MODIFIED_DATE, 
	MODIFIED_USER_ID, 
	EXTRACT_DATE, 
	AS_OF_DATE, 
	RECORD_COUNT, 
	SOURCE_SYSTEM_ID, 
	AUDIT_ID_OP AS AUDIT_ID
	FROM EXP_arch_sup_claim_category_stage
),