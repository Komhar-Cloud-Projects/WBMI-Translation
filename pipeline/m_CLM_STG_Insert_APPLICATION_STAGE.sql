WITH
SQ_application AS (
	SELECT 
		app.app_guid, 
		app.app_template_id, 
		app.display_name, 
		app.published_to_prod_flag, 
		app.enabled_flag, 
		app.version_num, 
		app.created_user_id, 
		app.created_date, 
		app.modified_user_id, 
		app.modified_date, 
		app.eff_date, 
		app.exp_date
	FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.application AS app WITH (NOLOCK)
	WHERE app.created_date >= '@{pipeline().parameters.SELECTION_START_TS}'
	          OR app.modified_date >= '@{pipeline().parameters.SELECTION_END_TS}'
),
EXP_SOURCE AS (
	SELECT
	app_guid,
	app_template_id,
	display_name,
	published_to_prod_flag,
	enabled_flag,
	version_num,
	created_user_id,
	created_date,
	modified_user_id,
	modified_date,
	eff_date,
	exp_date,
	sysdate AS extract_date,
	sysdate AS as_of_date,
	'' AS record_count,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS SOURCE_SYSTEM_ID
	FROM SQ_application
),
application_stage_INS AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.application_stage;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.application_stage
	(app_guid, app_template_id, display_name, published_to_prod_flag, enabled_flag, version_num, created_user_id, created_date, modified_user_id, modified_date, eff_date, exp_date, extract_date, as_of_date, record_count, source_system_id)
	SELECT 
	APP_GUID, 
	APP_TEMPLATE_ID, 
	DISPLAY_NAME, 
	PUBLISHED_TO_PROD_FLAG, 
	ENABLED_FLAG, 
	VERSION_NUM, 
	CREATED_USER_ID, 
	CREATED_DATE, 
	MODIFIED_USER_ID, 
	MODIFIED_DATE, 
	EFF_DATE, 
	EXP_DATE, 
	EXTRACT_DATE, 
	AS_OF_DATE, 
	RECORD_COUNT, 
	SOURCE_SYSTEM_ID AS SOURCE_SYSTEM_ID
	FROM EXP_SOURCE
),