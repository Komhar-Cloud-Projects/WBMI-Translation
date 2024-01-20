WITH
SQ_sup_assoc_plus AS (
	SELECT
		code,
		state_type_code,
		agency_state_num,
		agency_pay_type,
		agency_num,
		eff_date,
		exp_date,
		descript,
		modified_date,
		modified_user_id
	FROM sup_assoc_plus
),
EXP_SUP_ASSOC_PLUS AS (
	SELECT
	code,
	state_type_code,
	agency_state_num,
	agency_pay_type,
	agency_num,
	eff_date,
	exp_date,
	descript,
	modified_date,
	modified_user_id,
	SYSDATE AS EXTRACT_DATE,
	SYSDATE AS AS_OF_DATE,
	'' AS RECORD_COUNT,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS SOURCE_SYSTEM_ID
	FROM SQ_sup_assoc_plus
),
sup_assoc_plus_stage AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_assoc_plus_stage;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_assoc_plus_stage
	(code, state_type_code, agency_state_num, agency_pay_type, agency_num, eff_date, exp_date, descript, modified_date, modified_user_id, extract_date, as_of_date, record_count, source_system_id)
	SELECT 
	CODE, 
	STATE_TYPE_CODE, 
	AGENCY_STATE_NUM, 
	AGENCY_PAY_TYPE, 
	AGENCY_NUM, 
	EFF_DATE, 
	EXP_DATE, 
	DESCRIPT, 
	MODIFIED_DATE, 
	MODIFIED_USER_ID, 
	EXTRACT_DATE AS EXTRACT_DATE, 
	AS_OF_DATE AS AS_OF_DATE, 
	RECORD_COUNT AS RECORD_COUNT, 
	SOURCE_SYSTEM_ID AS SOURCE_SYSTEM_ID
	FROM EXP_SUP_ASSOC_PLUS
),