WITH
SQ_sup_acc_violation AS (
	SELECT
		code,
		eff_date,
		state_type_code,
		exp_date,
		pmsc_code,
		descript,
		major_minor_flag,
		amt_requ_flag,
		descript_requ_flag,
		driver_requ_type,
		cov_part_type_code,
		modified_date,
		modified_user_id,
		policy_level_claim_flag,
		attr_severity_code
	FROM sup_acc_violation
),
EXP_SUP_ACC_VIOLATION AS (
	SELECT
	code,
	eff_date,
	state_type_code,
	exp_date,
	pmsc_code,
	descript,
	major_minor_flag,
	amt_requ_flag,
	descript_requ_flag,
	driver_requ_type,
	cov_part_type_code,
	modified_date,
	modified_user_id,
	policy_level_claim_flag,
	attr_severity_code,
	SYSDATE AS EXTRACT_DATE,
	SYSDATE AS AS_OF_DATE,
	'' AS RECORD_COUNT,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS SOURCE_SYSTEM_ID
	FROM SQ_sup_acc_violation
),
sup_acc_violation_stage AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_acc_violation_stage;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_acc_violation_stage
	(code, eff_date, state_type_code, exp_date, pmsc_code, descript, major_minor_flag, amt_requ_flag, descript_requ_flag, driver_requ_type, cov_part_type_code, modified_date, modified_user_id, policy_level_claim_flag, attr_severity_code, extract_date, as_of_date, record_count, source_system_id)
	SELECT 
	CODE, 
	EFF_DATE, 
	STATE_TYPE_CODE, 
	EXP_DATE, 
	PMSC_CODE, 
	DESCRIPT, 
	MAJOR_MINOR_FLAG, 
	AMT_REQU_FLAG, 
	DESCRIPT_REQU_FLAG, 
	DRIVER_REQU_TYPE, 
	COV_PART_TYPE_CODE, 
	MODIFIED_DATE, 
	MODIFIED_USER_ID, 
	POLICY_LEVEL_CLAIM_FLAG, 
	ATTR_SEVERITY_CODE, 
	EXTRACT_DATE AS EXTRACT_DATE, 
	AS_OF_DATE AS AS_OF_DATE, 
	RECORD_COUNT AS RECORD_COUNT, 
	SOURCE_SYSTEM_ID AS SOURCE_SYSTEM_ID
	FROM EXP_SUP_ACC_VIOLATION
),