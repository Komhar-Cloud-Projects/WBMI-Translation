WITH
SQ_application_context_stage AS (
	SELECT 
		ac.application_context_stage_id, 
		ac.app_context_guid, 
		ac.app_guid, 
		ac.app_context_ent_name, 
		ac.display_name, 
		ac.created_user_id, 
		ac.created_date, 
		ac.modified_user_id, 
		ac.modified_date, 
		ac.extract_date, 
		ac.as_of_date, 
		ac.record_count, 
		ac.source_system_id
	FROM   @{pipeline().parameters.SOURCE_TABLE_OWNER}.application_context_stage ac WITH (NOLOCK)
),
EXP_SOURCE AS (
	SELECT
	application_context_stage_id,
	app_context_guid,
	app_guid,
	app_context_ent_name,
	display_name,
	created_user_id,
	created_date,
	modified_user_id,
	modified_date,
	extract_date,
	as_of_date,
	record_count,
	source_system_id,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id
	FROM SQ_application_context_stage
),
arch_application_context_stage_INS AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.arch_application_context_stage
	(application_context_stage_id, app_context_guid, app_guid, app_context_ent_name, display_name, created_user_id, created_date, modified_user_id, modified_date, extract_date, as_of_date, record_count, source_system_id, audit_id)
	SELECT 
	APPLICATION_CONTEXT_STAGE_ID, 
	APP_CONTEXT_GUID, 
	APP_GUID, 
	APP_CONTEXT_ENT_NAME, 
	DISPLAY_NAME, 
	CREATED_USER_ID, 
	CREATED_DATE, 
	MODIFIED_USER_ID, 
	MODIFIED_DATE, 
	EXTRACT_DATE, 
	AS_OF_DATE, 
	RECORD_COUNT, 
	SOURCE_SYSTEM_ID, 
	AUDIT_ID
	FROM EXP_SOURCE
),