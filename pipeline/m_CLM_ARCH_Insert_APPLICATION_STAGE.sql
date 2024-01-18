WITH
SQ_application_stage AS (
	SELECT 
		a.application_stage_id, 
		a.app_guid, 
		a.app_template_id, 
		a.display_name, 
		a.published_to_prod_flag, 
		a.enabled_flag, 
		a.version_num, 
		a.created_user_id, 
		a.created_date, 
		a.modified_user_id, 
		a.modified_date, 
		a.eff_date, 
		a.exp_date, 
		a.extract_date, 
		a.as_of_date, 
		a.record_count, 
		a.source_system_id
	FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.application_stage 
	AS A WITH (NOLOCK)
),
EXP_SOURCE AS (
	SELECT
	application_stage_id,
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
	extract_date,
	as_of_date,
	record_count,
	source_system_id,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id
	FROM SQ_application_stage
),
arch_application_stage_INS AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.arch_application_stage
	(application_stage_id, app_guid, app_template_id, display_name, published_to_prod_flag, enabled_flag, version_num, created_user_id, created_date, modified_user_id, modified_date, eff_date, exp_date, extract_date, as_of_date, record_count, source_system_id, audit_id)
	SELECT 
	APPLICATION_STAGE_ID, 
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
	SOURCE_SYSTEM_ID, 
	AUDIT_ID
	FROM EXP_SOURCE
),