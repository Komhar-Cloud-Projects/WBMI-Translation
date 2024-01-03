WITH
SQ_wbmi_audit_control_run_EXPIRE_AUDIT AS (
	SELECT wbmi_audit_control_run.wbmi_audit_control_run_id 
	FROM
	 wbmi_audit_control_run as wbmi_audit_control_run
	WHERE audit_name = '@{pipeline().parameters.AUDIT_NAME}'
	AND wbmi_audit_control_run.current_indicator = 'Y'
	AND NOT EXISTS(SELECT 1
		FROM wbmi_audit_control_run as wbmi_audit_control_run2
		WHERE wbmi_audit_control_run.wbmi_audit_control_id =  wbmi_audit_control_run2.wbmi_audit_control_id
	      and audit_name = '@{pipeline().parameters.AUDIT_NAME}'
	      AND wbmi_audit_control_run.current_indicator = 'Y'
		AND wbmi_audit_control_run2.end_ts IS NULL)
	
	--Subquery in place to ensure that if we have not ran m_Do_PostAudit for an Audit that m_Do_PreAudit
	--does not run and initialize a new set of rows in the corresponding run tables.
),
EXP_wbmi_audit_control_run_EXPIRE_AUDIT AS (
	SELECT
	wbmi_audit_control_run_id,
	'N' AS current_indicator,
	sysdate AS modified_date
	FROM SQ_wbmi_audit_control_run_EXPIRE_AUDIT
),
UPD_wbmi_audit_control_run_EXPIRE_AUDIT AS (
	SELECT
	wbmi_audit_control_run_id, 
	current_indicator, 
	modified_date
	FROM EXP_wbmi_audit_control_run_EXPIRE_AUDIT
),
wbmi_audit_control_run_EXPIRE_AUDIT AS (
	MERGE INTO wbmi_audit_control_run AS T
	USING UPD_wbmi_audit_control_run_EXPIRE_AUDIT AS S
	ON T.wbmi_audit_control_run_id = S.wbmi_audit_control_run_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.current_indicator = S.current_indicator, T.modified_date = S.modified_date
),
SQ_wbmi_audit_control_INITIALIZE_NEW_AUDIT AS (
	SELECT 
	wbmi_audit_control.wbmi_audit_control_id, 
	wbmi_audit_control.audit_name, 
	wbmi_audit_control.audit_notes, 
	wbmi_audit_control.created_user_id, 
	wbmi_audit_control.created_date, 
	wbmi_audit_control.modified_user_id 
	FROM
	 wbmi_audit_control
	WHERE audit_name = '@{pipeline().parameters.AUDIT_NAME}'
	AND NOT EXISTS(SELECT 1
		FROM wbmi_audit_control_run as wbmi_audit_control_run2
		WHERE wbmi_audit_control.wbmi_audit_control_id =  wbmi_audit_control_run2.wbmi_audit_control_id
	       and audit_name = '@{pipeline().parameters.AUDIT_NAME}'
		AND wbmi_audit_control_run2.end_ts IS NULL)
	
	--Subquery in place to ensure that if we have not ran m_Do_PostAudit for an Audit that m_Do_PreAudit
	--does not run and initialize a new set of rows in the corresponding run tables.
),
EXP_wbmi_audit_control_run AS (
	SELECT
	wbmi_audit_control_id,
	audit_name,
	audit_notes,
	'Y' AS current_indicator,
	sysdate AS start_ts,
	created_user_id,
	created_date,
	modified_user_id,
	sysdate AS modified_date
	FROM SQ_wbmi_audit_control_INITIALIZE_NEW_AUDIT
),
UPD_wbmi_audit_control_run AS (
	SELECT
	wbmi_audit_control_id, 
	audit_name, 
	audit_notes, 
	current_indicator, 
	start_ts, 
	created_user_id, 
	created_date, 
	modified_user_id, 
	modified_date
	FROM EXP_wbmi_audit_control_run
),
wbmi_audit_control_run_INITIALIZE_NEW_AUDIT AS (
	INSERT INTO wbmi_audit_control_run
	(wbmi_audit_control_id, audit_name, audit_notes, current_indicator, start_ts, created_user_id, created_date, modified_user_id, modified_date)
	SELECT 
	WBMI_AUDIT_CONTROL_ID, 
	AUDIT_NAME, 
	AUDIT_NOTES, 
	CURRENT_INDICATOR, 
	START_TS, 
	CREATED_USER_ID, 
	CREATED_DATE, 
	MODIFIED_USER_ID, 
	MODIFIED_DATE
	FROM UPD_wbmi_audit_control_run
),