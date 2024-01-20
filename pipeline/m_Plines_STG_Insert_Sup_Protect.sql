WITH
SQ_sup_protect AS (
	SELECT
		code,
		eff_date,
		exp_date,
		descript,
		modified_date,
		modified_user_id
	FROM sup_protect
),
EXP_SUP_PROTECT AS (
	SELECT
	code,
	eff_date,
	exp_date,
	descript,
	modified_date,
	modified_user_id,
	SYSDATE AS EXTRACT_DATE,
	SYSDATE AS AS_OF_DATE,
	'' AS RECORD_COUNT,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS SOURCE_SYSTEM_ID
	FROM SQ_sup_protect
),
sup_protect_stage AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_protect_stage;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_protect_stage
	(code, eff_date, exp_date, descript, modified_date, modified_user_id, extract_date, as_of_date, record_count, source_system_id)
	SELECT 
	CODE, 
	EFF_DATE, 
	EXP_DATE, 
	DESCRIPT, 
	MODIFIED_DATE, 
	MODIFIED_USER_ID, 
	EXTRACT_DATE AS EXTRACT_DATE, 
	AS_OF_DATE AS AS_OF_DATE, 
	RECORD_COUNT AS RECORD_COUNT, 
	SOURCE_SYSTEM_ID AS SOURCE_SYSTEM_ID
	FROM EXP_SUP_PROTECT
),