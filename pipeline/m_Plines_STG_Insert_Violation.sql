WITH
SQ_violation AS (
	SELECT
		violation_seq,
		person_seq,
		cov_part_type_code,
		policy_num,
		policy_sym,
		policy_mod,
		policy_mco,
		violation_num,
		date,
		acc_violation_type_code,
		descript,
		amt,
		modified_date,
		modified_user_id,
		claim_status,
		rpt_type_code,
		endorsement_view_code,
		updater_acc_violation_type_code,
		comp_claim_type_code
	FROM violation
),
EXP_VIOLATION AS (
	SELECT
	violation_seq,
	person_seq,
	cov_part_type_code,
	policy_num,
	policy_sym,
	policy_mod,
	policy_mco,
	violation_num,
	date,
	acc_violation_type_code,
	descript,
	amt,
	modified_date,
	modified_user_id,
	claim_status,
	rpt_type_code,
	endorsement_view_code,
	updater_acc_violation_type_code,
	comp_claim_type_code,
	SYSDATE AS EXTRACT_DATE,
	SYSDATE AS AS_OF_DATE,
	'' AS RECORD_COUNT,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS SOURCE_SYSTEM_ID
	FROM SQ_violation
),
violation_stage AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.violation_stage;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.violation_stage
	(violation_seq, person_seq, cov_part_type_code, policy_num, policy_sym, policy_mod, policy_mco, violation_num, date, acc_violation_type_code, descript, amt, modified_date, modified_user_id, claim_status, rpt_type_code, endorsement_view_code, updater_acc_violation_type_code, comp_claim_type_code, extract_date, as_of_date, record_count, source_system_id)
	SELECT 
	VIOLATION_SEQ, 
	PERSON_SEQ, 
	COV_PART_TYPE_CODE, 
	POLICY_NUM, 
	POLICY_SYM, 
	POLICY_MOD, 
	POLICY_MCO, 
	VIOLATION_NUM, 
	DATE, 
	ACC_VIOLATION_TYPE_CODE, 
	DESCRIPT, 
	AMT, 
	MODIFIED_DATE, 
	MODIFIED_USER_ID, 
	CLAIM_STATUS, 
	RPT_TYPE_CODE, 
	ENDORSEMENT_VIEW_CODE, 
	UPDATER_ACC_VIOLATION_TYPE_CODE, 
	COMP_CLAIM_TYPE_CODE, 
	EXTRACT_DATE AS EXTRACT_DATE, 
	AS_OF_DATE AS AS_OF_DATE, 
	RECORD_COUNT AS RECORD_COUNT, 
	SOURCE_SYSTEM_ID AS SOURCE_SYSTEM_ID
	FROM EXP_VIOLATION
),