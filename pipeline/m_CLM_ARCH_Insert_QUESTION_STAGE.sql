WITH
SQ_question_stage AS (
	SELECT 
		q.question_stage_id, 
		q.question_guid, 
		q.parent_question_guid, 
		q.optn_set_guid, 
		q.applicability_filter_guid, 
		q.app_context_guid, 
		q.app_context_grp_guid, 
		q.display_name, 
		q.logical_name, 
		q.published_to_prod_flag, 
		q.enabled_flag, 
		q.help_text, 
		q.prompt, 
		q.question_template_id, 
		q.settings,
		q.traceability_id, 
		q.triggers, 
		q.sort_order, 
		q.notes,
		q.surrogate_question_guid, 
		q.created_user_id, 
		q.created_date, 
		q.modified_user_id, 
		q.modified_date, 
		q.eff_date,
		q.exp_date, 
		q.extract_date, 
		q.as_of_date,
		q.record_count, 
		q.source_system_id
	FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.question_stage AS q WITH (NOLOCK)
),
EXP_SOURCE AS (
	SELECT
	question_stage_id,
	question_guid,
	parent_question_guid,
	optn_set_guid,
	applicability_filter_guid,
	app_context_guid,
	app_context_grp_guid,
	display_name,
	logical_name,
	published_to_prod_flag,
	enabled_flag,
	help_text,
	prompt,
	question_template_id,
	settings,
	traceability_id,
	triggers,
	sort_order,
	notes,
	surrogate_question_guid,
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
	FROM SQ_question_stage
),
arch_question_stage_INS AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.arch_question_stage
	(question_stage_id, question_guid, parent_question_guid, optn_set_guid, applicability_filter_guid, app_context_guid, app_context_grp_guid, display_name, logical_name, published_to_prod_flag, enabled_flag, help_text, prompt, question_template_id, settings, traceability_id, triggers, sort_order, notes, surrogate_question_guid, created_user_id, created_date, modified_user_id, modified_date, eff_date, exp_date, extract_date, as_of_date, record_count, source_system_id, audit_id)
	SELECT 
	QUESTION_STAGE_ID, 
	QUESTION_GUID, 
	PARENT_QUESTION_GUID, 
	OPTN_SET_GUID, 
	APPLICABILITY_FILTER_GUID, 
	APP_CONTEXT_GUID, 
	APP_CONTEXT_GRP_GUID, 
	DISPLAY_NAME, 
	LOGICAL_NAME, 
	PUBLISHED_TO_PROD_FLAG, 
	ENABLED_FLAG, 
	HELP_TEXT, 
	PROMPT, 
	QUESTION_TEMPLATE_ID, 
	SETTINGS, 
	TRACEABILITY_ID, 
	TRIGGERS, 
	SORT_ORDER, 
	NOTES, 
	SURROGATE_QUESTION_GUID, 
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