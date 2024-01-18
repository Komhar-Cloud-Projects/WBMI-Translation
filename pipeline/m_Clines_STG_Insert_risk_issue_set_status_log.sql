WITH
SQ_risk_issue_set_status_log AS (
	SELECT
		risk_issue_set_status_log_id,
		risk_issue_set_id,
		risk_issue_set_status_id,
		note,
		created_user_urn,
		created_user_id,
		created_date,
		modified_user_id,
		modified_date
	FROM risk_issue_set_status_log
),
EXP_Values AS (
	SELECT
	risk_issue_set_status_log_id,
	risk_issue_set_id,
	risk_issue_set_status_id,
	note,
	created_user_urn,
	created_user_id,
	created_date,
	modified_user_id,
	modified_date,
	SYSDATE AS extract_date,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS source_system_id
	FROM SQ_risk_issue_set_status_log
),
risk_issue_set_status_log_cl_stage AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.risk_issue_set_status_log_cl_stage;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.risk_issue_set_status_log_cl_stage
	(risk_issue_set_status_log_id, risk_issue_set_id, risk_issue_set_status_id, note, created_user_urn, created_user_id, created_date, modified_user_id, modified_date, extract_date, source_system_id)
	SELECT 
	RISK_ISSUE_SET_STATUS_LOG_ID, 
	RISK_ISSUE_SET_ID, 
	RISK_ISSUE_SET_STATUS_ID, 
	NOTE, 
	CREATED_USER_URN, 
	CREATED_USER_ID, 
	CREATED_DATE, 
	MODIFIED_USER_ID, 
	MODIFIED_DATE, 
	EXTRACT_DATE, 
	SOURCE_SYSTEM_ID
	FROM EXP_Values
),