WITH
SQ_sup_anti_theft AS (
	SELECT
		code,
		eff_date,
		exp_date,
		state_type_code,
		pmsc_category,
		descript,
		modified_date,
		modified_user_id,
		descript_long,
		discount_percent
	FROM sup_anti_theft
),
EXP_SUP_ANTI_THEFT AS (
	SELECT
	code,
	eff_date,
	exp_date,
	state_type_code,
	pmsc_category,
	descript,
	modified_date,
	modified_user_id,
	descript_long,
	discount_percent,
	SYSDATE AS EXTRACT_DATE,
	SYSDATE AS AS_OF_DATE,
	'' AS RECORD_COUNT,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS SOURCE_SYSTEM_ID
	FROM SQ_sup_anti_theft
),
sup_anti_theft_stage AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_anti_theft_stage;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_anti_theft_stage
	(code, eff_date, exp_date, state_type_code, pmsc_category, descript, modified_date, modified_user_id, descript_long, discount_percent, extract_date, as_of_date, record_count, source_system_id)
	SELECT 
	CODE, 
	EFF_DATE, 
	EXP_DATE, 
	STATE_TYPE_CODE, 
	PMSC_CATEGORY, 
	DESCRIPT, 
	MODIFIED_DATE, 
	MODIFIED_USER_ID, 
	DESCRIPT_LONG, 
	DISCOUNT_PERCENT, 
	EXTRACT_DATE AS EXTRACT_DATE, 
	AS_OF_DATE AS AS_OF_DATE, 
	RECORD_COUNT AS RECORD_COUNT, 
	SOURCE_SYSTEM_ID AS SOURCE_SYSTEM_ID
	FROM EXP_SUP_ANTI_THEFT
),