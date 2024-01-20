WITH
SQ_coverage_category_stage AS (
	SELECT 
	c.cov_ctgry_stage_id, 
	c.cov_category_code, 
	c.cov_category_descript, 
	c.modified_date, 
	c.modified_user_id, 
	c.extract_date, 
	c.source_sys_id 
	FROM
	 @{pipeline().parameters.SOURCE_TABLE_OWNER}.coverage_category_stage  c
	WHERE NOT EXISTS
	(SELECT 'X'
	FROM  @{pipeline().parameters.TARGET_TABLE_OWNER}.arch_coverage_category_stage a
	WHERE a.cov_category_code = c.cov_category_code
	AND  a.cov_category_descript = c.cov_category_descript)
),
EXP_COVERAGE_CATEGORY AS (
	SELECT
	cov_ctgry_stage_id,
	cov_category_code,
	cov_category_descript,
	modified_date,
	modified_user_id,
	extract_date AS EXTRACT_DATE,
	source_sys_id AS SOURCE_SYSTEM_ID,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AUDIT_ID_OP
	FROM SQ_coverage_category_stage
),
arch_coverage_category_stage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.arch_coverage_category_stage
	(cov_ctgry_stage_id, cov_category_code, cov_category_descript, modified_date, modified_user_id, extract_date, source_sys_id, audit_id)
	SELECT 
	COV_CTGRY_STAGE_ID, 
	COV_CATEGORY_CODE, 
	COV_CATEGORY_DESCRIPT, 
	MODIFIED_DATE, 
	MODIFIED_USER_ID, 
	EXTRACT_DATE AS EXTRACT_DATE, 
	SOURCE_SYSTEM_ID AS SOURCE_SYS_ID, 
	AUDIT_ID_OP AS AUDIT_ID
	FROM EXP_COVERAGE_CATEGORY
),