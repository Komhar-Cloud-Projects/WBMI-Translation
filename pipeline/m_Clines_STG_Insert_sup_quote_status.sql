WITH
SQ_sup_quote_status AS (
	SELECT
		quote_status_id,
		quote_status,
		created_user_id,
		created_date,
		modified_user_id,
		modified_date,
		eff_date,
		exp_date
	FROM sup_quote_status
),
EXP_Values AS (
	SELECT
	quote_status_id,
	quote_status,
	created_user_id,
	created_date,
	modified_user_id,
	modified_date,
	eff_date,
	exp_date,
	SYSDATE AS extract_date,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS source_system_id
	FROM SQ_sup_quote_status
),
sup_quote_status_cl_stage AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_quote_status_cl_stage;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_quote_status_cl_stage
	(quote_status_id, quote_status, created_user_id, created_date, modified_user_id, modified_date, eff_date, exp_date, extract_date, source_system_id)
	SELECT 
	QUOTE_STATUS_ID, 
	QUOTE_STATUS, 
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