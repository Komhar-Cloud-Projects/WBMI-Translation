WITH
SQ_coverage_master AS (
	SELECT
		cov_master_seq,
		policy_num,
		policy_sym,
		policy_mod,
		policy_mco,
		cov_part_seq,
		cov_type_code,
		modified_date,
		modified_user_id,
		cov_part_type_code
	FROM coverage_master
	WHERE coverage_master.cov_master_seq%3=0
),
EXP_COVERAGE_MASTER AS (
	SELECT
	cov_master_seq,
	policy_num,
	policy_sym,
	policy_mod,
	policy_mco,
	cov_part_seq,
	cov_type_code,
	modified_date,
	modified_user_id,
	cov_part_type_code,
	SYSDATE AS EXTRACT_DATE,
	SYSDATE AS AS_OF_DATE,
	'' AS RECORD_COUNT,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS SOURCE_SYSTEM_ID
	FROM SQ_coverage_master
),
coverage_master_stage AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.coverage_master_stage;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.coverage_master_stage
	(cov_master_seq, policy_num, policy_sym, policy_mod, policy_mco, cov_part_seq, cov_type_code, modified_date, modified_user_id, cov_part_type_code, extract_date, as_of_date, record_count, source_system_id)
	SELECT 
	COV_MASTER_SEQ, 
	POLICY_NUM, 
	POLICY_SYM, 
	POLICY_MOD, 
	POLICY_MCO, 
	COV_PART_SEQ, 
	COV_TYPE_CODE, 
	MODIFIED_DATE, 
	MODIFIED_USER_ID, 
	COV_PART_TYPE_CODE, 
	EXTRACT_DATE AS EXTRACT_DATE, 
	AS_OF_DATE AS AS_OF_DATE, 
	RECORD_COUNT AS RECORD_COUNT, 
	SOURCE_SYSTEM_ID AS SOURCE_SYSTEM_ID
	FROM EXP_COVERAGE_MASTER
),