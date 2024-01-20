WITH
SQ_sup_cov AS (
	SELECT
		code,
		cov_part_type_code,
		eff_date,
		exp_date,
		pmsc_code,
		pmsc_base,
		descript,
		modified_date,
		modified_user_id,
		master_cov_type
	FROM sup_cov
),
EXP_COV AS (
	SELECT
	code,
	cov_part_type_code,
	eff_date,
	exp_date,
	pmsc_code,
	pmsc_base,
	descript,
	modified_date,
	modified_user_id,
	master_cov_type,
	SYSDATE AS EXTRACT_DATE,
	SYSDATE AS AS_OF_DATE,
	'' AS RECORD_COUNT,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS SOURCE_SYSTEM_ID
	FROM SQ_sup_cov
),
sup_cov_stage AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_cov_stage;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_cov_stage
	(code, cov_part_type_code, eff_date, exp_date, pmsc_code, pmsc_base, descript, modified_date, modified_user_id, master_cov_type, extract_date, as_of_date, record_count, source_system_id)
	SELECT 
	CODE, 
	COV_PART_TYPE_CODE, 
	EFF_DATE, 
	EXP_DATE, 
	PMSC_CODE, 
	PMSC_BASE, 
	DESCRIPT, 
	MODIFIED_DATE, 
	MODIFIED_USER_ID, 
	MASTER_COV_TYPE, 
	EXTRACT_DATE AS EXTRACT_DATE, 
	AS_OF_DATE AS AS_OF_DATE, 
	RECORD_COUNT AS RECORD_COUNT, 
	SOURCE_SYSTEM_ID AS SOURCE_SYSTEM_ID
	FROM EXP_COV
),