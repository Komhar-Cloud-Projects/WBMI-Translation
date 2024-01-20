WITH
SQ_sup_wage_basis_stage AS (
	SELECT
		sup_wage_basis_stage_id,
		wage_basis_code,
		wage_basis_desc,
		modified_date,
		modified_user_id,
		extract_date,
		as_of_date,
		record_count,
		source_system_id
	FROM sup_wage_basis_stage
),
EXP_AUDIT_FIELDS AS (
	SELECT
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AUDIT_ID_OP,
	sup_wage_basis_stage_id,
	wage_basis_code,
	wage_basis_desc,
	modified_date,
	modified_user_id,
	extract_date,
	as_of_date,
	record_count,
	source_system_id
	FROM SQ_sup_wage_basis_stage
),
arch_sup_wage_basis_stage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.arch_sup_wage_basis_stage
	(sup_wage_basis_stage_id, wage_basis_code, wage_basis_desc, modified_date, modified_user_id, extract_date, as_of_date, record_count, source_system_id, audit_id)
	SELECT 
	SUP_WAGE_BASIS_STAGE_ID, 
	WAGE_BASIS_CODE, 
	WAGE_BASIS_DESC, 
	MODIFIED_DATE, 
	MODIFIED_USER_ID, 
	EXTRACT_DATE, 
	AS_OF_DATE, 
	RECORD_COUNT, 
	SOURCE_SYSTEM_ID, 
	AUDIT_ID_OP AS AUDIT_ID
	FROM EXP_AUDIT_FIELDS
),