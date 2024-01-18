WITH
SQ_log_note_stage AS (
	SELECT log_note_stage.log_note_stage_id, log_note_stage.note_id, log_note_stage.claim_id, log_note_stage.author_user_id, log_note_stage.author_name, log_note_stage.deleted_user_name, log_note_stage.note_text, log_note_stage.create_date, log_note_stage.deleted_date, log_note_stage.deleted_user_id, log_note_stage.viewable_flag, log_note_stage.notify_uw, log_note_stage.note_type, log_note_stage.notify_siu, log_note_stage.notify_collection, log_note_stage.extract_date, log_note_stage.as_of_date, log_note_stage.record_count, log_note_stage.source_system_id 
	FROM
	 log_note_stage
	WHERE 
	log_note_stage.create_date >= '@{pipeline().parameters.SELECTION_START_TS}'
	OR
	log_note_stage.deleted_date >= '@{pipeline().parameters.SELECTION_START_TS}'
),
EXP_LOG_NOTE AS (
	SELECT
	log_note_stage_id,
	note_id,
	claim_id,
	author_user_id,
	author_name,
	deleted_user_name,
	note_text,
	create_date,
	deleted_date,
	deleted_user_id,
	viewable_flag,
	notify_uw,
	note_type,
	notify_siu,
	notify_collection,
	extract_date,
	as_of_date,
	record_count,
	source_system_id,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AUDIT_ID_OP
	FROM SQ_log_note_stage
),
arch_log_note_stage AS (
	INSERT INTO arch_log_note_stage
	(log_note_stage_id, note_id, claim_id, author_user_id, author_name, deleted_user_name, note_text, create_date, deleted_date, deleted_user_id, viewable_flag, notify_uw, note_type, notify_siu, notify_collection, extract_date, as_of_date, record_count, source_system_id, audit_id)
	SELECT 
	LOG_NOTE_STAGE_ID, 
	NOTE_ID, 
	CLAIM_ID, 
	AUTHOR_USER_ID, 
	AUTHOR_NAME, 
	DELETED_USER_NAME, 
	NOTE_TEXT, 
	CREATE_DATE, 
	DELETED_DATE, 
	DELETED_USER_ID, 
	VIEWABLE_FLAG, 
	NOTIFY_UW, 
	NOTE_TYPE, 
	NOTIFY_SIU, 
	NOTIFY_COLLECTION, 
	EXTRACT_DATE, 
	AS_OF_DATE, 
	RECORD_COUNT, 
	SOURCE_SYSTEM_ID, 
	AUDIT_ID_OP AS AUDIT_ID
	FROM EXP_LOG_NOTE
),