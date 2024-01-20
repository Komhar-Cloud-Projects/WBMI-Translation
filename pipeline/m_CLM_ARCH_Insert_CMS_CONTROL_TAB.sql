WITH
SQ_cms_control_tab_stage AS (
	SELECT cms_control_tab_stage.cms_control_tab_stage_id, cms_control_tab_stage.cms_doc_cntl_num, cms_control_tab_stage.cms_report_status, cms_control_tab_stage.cms_report_date, cms_control_tab_stage.cms_action_type, cms_control_tab_stage.created_ts, cms_control_tab_stage.created_user_id, cms_control_tab_stage.modified_ts, cms_control_tab_stage.modified_user_id, cms_control_tab_stage.extract_date, cms_control_tab_stage.as_of_date, cms_control_tab_stage.record_count, cms_control_tab_stage.source_system_id 
	FROM
	 cms_control_tab_stage
	WHERE
	cms_control_tab_stage.created_ts >= '@{pipeline().parameters.SELECTION_START_TS}'
	OR
	cms_control_tab_stage.modified_ts >= '@{pipeline().parameters.SELECTION_START_TS}'
),
EXPTRANS AS (
	SELECT
	cms_control_tab_stage_id,
	cms_doc_cntl_num,
	cms_report_status,
	cms_report_date,
	cms_action_type,
	created_ts,
	created_user_id,
	modified_ts,
	modified_user_id,
	extract_date,
	as_of_date,
	record_count,
	source_system_id,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AUDIT_ID_OP
	FROM SQ_cms_control_tab_stage
),
arch_cms_control_tab_stage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.arch_cms_control_tab_stage
	(cms_control_tab_stage_id, cms_doc_cntl_num, cms_report_status, cms_report_date, cms_action_type, created_ts, created_user_id, modified_ts, modified_user_id, extract_date, as_of_date, record_count, source_system_id, audit_id)
	SELECT 
	CMS_CONTROL_TAB_STAGE_ID, 
	CMS_DOC_CNTL_NUM, 
	CMS_REPORT_STATUS, 
	CMS_REPORT_DATE, 
	CMS_ACTION_TYPE, 
	CREATED_TS, 
	CREATED_USER_ID, 
	MODIFIED_TS, 
	MODIFIED_USER_ID, 
	EXTRACT_DATE, 
	AS_OF_DATE, 
	RECORD_COUNT, 
	SOURCE_SYSTEM_ID, 
	AUDIT_ID_OP AS AUDIT_ID
	FROM EXPTRANS
),