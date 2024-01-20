WITH
SQ_sup_risk_issue_set_status AS (
	SELECT
		risk_issue_set_status_id,
		risk_issue_set_status,
		created_user_id,
		created_date,
		modified_user_id,
		modified_date,
		eff_date,
		exp_date
	FROM sup_risk_issue_set_status
),
EXP_Values AS (
	SELECT
	risk_issue_set_status_id,
	risk_issue_set_status,
	created_user_id,
	created_date,
	modified_user_id,
	modified_date,
	eff_date,
	exp_date,
	SYSDATE AS extract_date,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS source_system_id
	FROM SQ_sup_risk_issue_set_status
),
sup_risk_issue_set_status_cl_stage AS (
	TRUNCATE TABLE @{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_risk_issue_set_status_cl_stage;
	INSERT INTO @{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_risk_issue_set_status_cl_stage
	(risk_issue_set_status_id, risk_issue_set_status, created_user_id, created_date, modified_user_id, modified_date, eff_date, exp_date, extract_date, source_system_id)
	SELECT 
	RISK_ISSUE_SET_STATUS_ID, 
	RISK_ISSUE_SET_STATUS, 
	CREATED_USER_ID, 
	CREATED_DATE, 
	MODIFIED_USER_ID, 
	MODIFIED_DATE, 
	EFF_DATE, 
	EXP_DATE, 
	EXTRACT_DATE, 
	SOURCE_SYSTEM_ID
	FROM EXP_Values
),