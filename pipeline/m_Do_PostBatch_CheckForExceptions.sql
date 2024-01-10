WITH
SRC_Rep_SessionLog AS (
	SELECT vw_REP_SESS_LOG.SUBJECT_AREA, vw_REP_SESS_LOG.SESSION_NAME, vw_REP_SESS_LOG.SUCCESSFUL_ROWS, vw_REP_SESS_LOG.FAILED_ROWS, vw_REP_SESS_LOG.FIRST_ERROR_CODE, vw_REP_SESS_LOG.FIRST_ERROR_MSG, vw_REP_SESS_LOG.LAST_ERROR_CODE, vw_REP_SESS_LOG.LAST_ERROR, vw_REP_SESS_LOG.RUN_STATUS_CODE, vw_REP_SESS_LOG.ACTUAL_START, vw_REP_SESS_LOG.SESSION_TIMESTAMP, vw_REP_SESS_LOG.WORKFLOW_NAME, vw_REP_SESS_LOG.MAPPING_NAME, vw_REP_SESS_LOG.TOTAL_ERR, vw_REP_SESS_LOG.WORKFLOW_RUN_ID, vw_REP_SESS_LOG.INSTANCE_ID 
	FROM
	 PowerCenter.vw_REP_SESS_LOG 
	WHERE
	 vw_REP_SESS_LOG.TOTAL_ERR <> 0 OR vw_REP_SESS_LOG.FAILED_ROWS <> 0
),
SRC_Session_batch_Control AS (
	SELECT wbmi_batch_control_run.wbmi_batch_control_run_id, wbmi_batch_control_run.batch_name, wbmi_batch_control_run.email_code, wbmi_batch_control_run.current_ind, wbmi_batch_control_run.email_address, wbmi_session_control_run.wbmi_session_control_id, wbmi_session_control_run.workflow_run_id, wbmi_session_control_run.sess_inst_id 
	FROM
	 wbmi_batch_control_run, wbmi_session_control_run 
	WHERE
	 wbmi_batch_control_run.batch_name = '@{pipeline().parameters.BATCH_NAME}'
	AND wbmi_batch_control_run.current_ind = 'Y' 
	AND
	 wbmi_batch_control_run.wbmi_batch_control_run_id=wbmi_session_control_run.wbmi_batch_control_run_id
),
JNR_Rep_Sess_Log_WBMI_Batch_ControlRun AS (SELECT
	SRC_Rep_SessionLog.SUBJECT_AREA, 
	SRC_Rep_SessionLog.SESSION_NAME, 
	SRC_Rep_SessionLog.SUCCESSFUL_ROWS, 
	SRC_Rep_SessionLog.FAILED_ROWS, 
	SRC_Rep_SessionLog.FIRST_ERROR_CODE, 
	SRC_Rep_SessionLog.FIRST_ERROR_MSG, 
	SRC_Rep_SessionLog.LAST_ERROR_CODE, 
	SRC_Rep_SessionLog.LAST_ERROR, 
	SRC_Rep_SessionLog.RUN_STATUS_CODE, 
	SRC_Rep_SessionLog.ACTUAL_START, 
	SRC_Rep_SessionLog.SESSION_TIMESTAMP, 
	SRC_Rep_SessionLog.WORKFLOW_NAME, 
	SRC_Rep_SessionLog.MAPPING_NAME, 
	SRC_Rep_SessionLog.TOTAL_ERR, 
	SRC_Rep_SessionLog.WORKFLOW_RUN_ID AS WORKFLOW_RUNID, 
	SRC_Rep_SessionLog.INSTANCE_ID, 
	SRC_Session_batch_Control.wbmi_batch_control_run_id, 
	SRC_Session_batch_Control.batch_name, 
	SRC_Session_batch_Control.email_code, 
	SRC_Session_batch_Control.current_ind, 
	SRC_Session_batch_Control.email_address, 
	SRC_Session_batch_Control.wbmi_session_control_id, 
	SRC_Session_batch_Control.workflow_run_id, 
	SRC_Session_batch_Control.sess_inst_id
	FROM SRC_Rep_SessionLog
	INNER JOIN SRC_Session_batch_Control
	ON SRC_Session_batch_Control.workflow_run_id = SRC_Rep_SessionLog.WORKFLOW_RUN_ID AND SRC_Session_batch_Control.sess_inst_id = SRC_Rep_SessionLog.INSTANCE_ID
),
EXP_Format_Email_Message AS (
	SELECT
	v_row_count AS v_row_count_temp,
	SESSION_NAME,
	SUCCESSFUL_ROWS,
	-- *INF*: TO_CHAR(SUCCESSFUL_ROWS)
	TO_CHAR(SUCCESSFUL_ROWS
	) AS v_SUCCESSFUL_ROWS_string,
	FAILED_ROWS,
	-- *INF*: TO_CHAR(FAILED_ROWS)
	TO_CHAR(FAILED_ROWS
	) AS v_FAILED_ROWS_string,
	FIRST_ERROR_CODE,
	-- *INF*: TO_CHAR(FIRST_ERROR_CODE)
	TO_CHAR(FIRST_ERROR_CODE
	) AS v_FIRST_ERROR_CODE_string,
	FIRST_ERROR_MSG,
	SESSION_TIMESTAMP,
	batch_name,
	email_address,
	-- *INF*: batch_name || ' Exceptions Occurred ' ||  TO_CHAR(SESSION_TIMESTAMP)
	batch_name || ' Exceptions Occurred ' || TO_CHAR(SESSION_TIMESTAMP
	) AS email_subject,
	v_row_count_temp + 1 AS v_row_count,
	-- *INF*: '<table width="100%" style="border-collapse: collapse" bordercolor="#111111" cellpadding="3" cellspacing="0">' || CHR(10) ||
	-- '	<tr><td width="176"><b><font face="Arial" size="2">Session Name</font></b></td>' || CHR(10) ||
	-- '      <td width="57"><b><font face="Arial" size="2">Target Success Rows</font></b></td>' || CHR(10) ||
	-- '      <td width="53"><b><font face="Arial" size="2">Target Failed Rows</font></b></td>' || CHR(10) ||
	-- '      <td width="884"><b><font face="Arial" size="2">First Error</font></b></td></tr>' || CHR(10) 
	'<table width="100%" style="border-collapse: collapse" bordercolor="#111111" cellpadding="3" cellspacing="0">' || CHR(10
	) || '	<tr><td width="176"><b><font face="Arial" size="2">Session Name</font></b></td>' || CHR(10
	) || '      <td width="57"><b><font face="Arial" size="2">Target Success Rows</font></b></td>' || CHR(10
	) || '      <td width="53"><b><font face="Arial" size="2">Target Failed Rows</font></b></td>' || CHR(10
	) || '      <td width="884"><b><font face="Arial" size="2">First Error</font></b></td></tr>' || CHR(10
	) AS v_email_body_header,
	-- *INF*: '	<tr><td width="176"><font face="Arial" size="2">' || SESSION_NAME || '</font></td>' ||
	-- '	<td width="57"><font face="Arial" size="2">' || v_SUCCESSFUL_ROWS_string || '</font></td>' ||
	-- '	<td width="53"><font face="Arial" size="2">'  || v_FAILED_ROWS_string || '</font></td>' ||
	-- '	<td width="884"><font face="Arial" size="1">'  || FIRST_ERROR_MSG || ' </font></td></tr>' || CHR(10)
	'	<tr><td width="176"><font face="Arial" size="2">' || SESSION_NAME || '</font></td>' || '	<td width="57"><font face="Arial" size="2">' || v_SUCCESSFUL_ROWS_string || '</font></td>' || '	<td width="53"><font face="Arial" size="2">' || v_FAILED_ROWS_string || '</font></td>' || '	<td width="884"><font face="Arial" size="1">' || FIRST_ERROR_MSG || ' </font></td></tr>' || CHR(10
	) AS v_email_body_content,
	-- *INF*: IIF(v_row_count = 1,
	-- 	v_email_body_header || CHR(10) || v_email_body_content,
	-- 	v_email_body_content)
	IFF(v_row_count = 1,
		v_email_body_header || CHR(10
		) || v_email_body_content,
		v_email_body_content
	) AS v_email_body,
	v_email_body AS out_email_body
	FROM JNR_Rep_Sess_Log_WBMI_Batch_ControlRun
),
AGG_Distinct_Email_Address_Subject AS (
	SELECT
	email_address,
	email_subject
	FROM EXP_Format_Email_Message
	QUALIFY ROW_NUMBER() OVER (PARTITION BY email_address, email_subject ORDER BY NULL) = 1
),
email_address AS (
	INSERT INTO email_address
	(FIELD1)
	SELECT 
	email_address AS FIELD1
	FROM AGG_Distinct_Email_Address_Subject
),
email_body AS (
	INSERT INTO email_body
	(FIELD1)
	SELECT 
	out_email_body AS FIELD1
	FROM EXP_Format_Email_Message
),
email_subject AS (
	INSERT INTO email_subject
	(FIELD1)
	SELECT 
	email_subject AS FIELD1
	FROM AGG_Distinct_Email_Address_Subject
),