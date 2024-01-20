WITH
SQ_application_context AS (
	SELECT
		app_con.app_context_guid, 
		app_con.app_guid, 
		app_con.app_context_ent_name, 
		app_con.display_name, 
		app_con.created_user_id, 
		app_con.created_date, 
		app_con.modified_user_id, 
		app_con.modified_date
	FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.application_context AS app_con WITH (NOLOCK)
	WHERE app_con.created_date >=  '@{pipeline().parameters.SELECTION_START_TS}'
	          OR app_con.modified_date >= '@{pipeline().parameters.SELECTION_START_TS}'
),
EXP_SOURCE AS (
	SELECT
	app_context_guid,
	app_guid,
	app_context_ent_name,
	display_name,
	created_user_id,
	created_date,
	modified_user_id,
	modified_date,
	sysdate AS EXTRACT_DATE,
	sysdate AS AS_OF_DATE,
	'' AS record_count,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS SOURCE_SYSTEM_ID
	FROM SQ_application_context
),
application_context_stage_INS AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.application_context_stage;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.application_context_stage
	(app_context_guid, app_guid, app_context_ent_name, display_name, created_user_id, created_date, modified_user_id, modified_date, extract_date, as_of_date, record_count, source_system_id)
	SELECT 
	APP_CONTEXT_GUID, 
	APP_GUID, 
	APP_CONTEXT_ENT_NAME, 
	DISPLAY_NAME, 
	CREATED_USER_ID, 
	CREATED_DATE, 
	MODIFIED_USER_ID, 
	MODIFIED_DATE, 
	EXTRACT_DATE AS EXTRACT_DATE, 
	AS_OF_DATE AS AS_OF_DATE, 
	RECORD_COUNT, 
	SOURCE_SYSTEM_ID AS SOURCE_SYSTEM_ID
	FROM EXP_SOURCE
),