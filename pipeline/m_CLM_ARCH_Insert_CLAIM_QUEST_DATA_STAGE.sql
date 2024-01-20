WITH
SQ_claim_quest_data_stage AS (
	SELECT 
		cqds.claim_quest_data_stage_id, 
		cqds.cqd_id, 
		cqds.claim_nbr, 
		cqds.claimant_id, 
		cqds.app_context_name, 
		cqds.question_guid, 
		cqds.quest_logical_name, 
		cqds.prompt, 
		cqds.optn_set_item_guid, 
		cqds.optn_set_item_val, 
		cqds.optn_text, 
		cqds.created_user_id, 
		cqds.created_date, 
		cqds.modified_user_id, 
		cqds.modified_date, 	 
		cqds.extract_date, 
		cqds.as_of_date, 
		cqds.record_count, 
		cqds.source_system_id
	FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_quest_data_stage AS cqds WITH (NOLOCK)
	WHERE cqds.CREATED_DATE >= '@{pipeline().parameters.SELECTION_START_TS}'
	 OR cqds.MODIFIED_DATE >= '@{pipeline().parameters.SELECTION_END_TS}'
),
EXP_SOURCE AS (
	SELECT
	claim_quest_data_stage_id,
	cqd_id,
	claim_nbr,
	claimant_id,
	app_context_name,
	question_guid,
	quest_logical_name,
	prompt,
	optn_set_item_guid,
	optn_set_item_val,
	optn_text,
	created_user_id,
	created_date,
	modified_user_id,
	modified_date,
	extract_date,
	as_of_date,
	record_count,
	source_system_id,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id
	FROM SQ_claim_quest_data_stage
),
arch_claim_quest_data_stage AS (
	INSERT INTO arch_claim_quest_data_stage
	(claim_quest_data_stage_id, cqd_id, claim_nbr, claimant_id, app_context_name, question_guid, quest_logical_name, prompt, optn_set_item_guid, optn_set_item_val, optn_text, created_user_id, created_date, modified_user_id, modified_date, extract_date, as_of_date, record_count, source_system_id, audit_id)
	SELECT 
	CLAIM_QUEST_DATA_STAGE_ID, 
	CQD_ID, 
	CLAIM_NBR, 
	CLAIMANT_ID, 
	APP_CONTEXT_NAME, 
	QUESTION_GUID, 
	QUEST_LOGICAL_NAME, 
	PROMPT, 
	OPTN_SET_ITEM_GUID, 
	OPTN_SET_ITEM_VAL, 
	OPTN_TEXT, 
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