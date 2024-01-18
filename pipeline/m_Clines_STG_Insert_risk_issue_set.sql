WITH
SQ_risk_issue_set AS (
	SELECT
		risk_issue_set_id,
		quote_id,
		risk_issue_set_type_id,
		assigned_user_urn,
		callback_endpoint_config_name,
		created_user_id,
		created_date,
		modified_user_id,
		modified_date
	FROM risk_issue_set
),
EXP_Values AS (
	SELECT
	risk_issue_set_id,
	quote_id,
	risk_issue_set_type_id,
	assigned_user_urn,
	callback_endpoint_config_name,
	created_user_id,
	created_date,
	modified_user_id,
	modified_date,
	SYSDATE AS extract_date,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS source_system_id
	FROM SQ_risk_issue_set
),
risk_issue_set_cl_stage AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.risk_issue_set_cl_stage;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.risk_issue_set_cl_stage
	(risk_issue_set_id, quote_id, risk_issue_set_type_id, assigned_user_urn, callback_endpoint_config_name, created_user_id, created_date, modified_user_id, modified_date, extract_date, source_system_id)
	SELECT 
	RISK_ISSUE_SET_ID, 
	QUOTE_ID, 
	RISK_ISSUE_SET_TYPE_ID, 
	ASSIGNED_USER_URN, 
	CALLBACK_ENDPOINT_CONFIG_NAME, 
	CREATED_USER_ID, 
	CREATED_DATE, 
	MODIFIED_USER_ID, 
	MODIFIED_DATE, 
	EXTRACT_DATE, 
	SOURCE_SYSTEM_ID
	FROM EXP_Values
),