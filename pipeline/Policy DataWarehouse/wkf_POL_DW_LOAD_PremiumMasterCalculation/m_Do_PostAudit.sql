WITH
SQ_wbmi_audit_control_run_EXPIRE AS (
	SELECT 
	wbmi_audit_control_run.wbmi_audit_control_run_id,
	wbmi_audit_control_run.start_ts 
	FROM
	 wbmi_audit_control_run
	WHERE audit_name = '@{pipeline().parameters.AUDIT_NAME}'
	  AND wbmi_audit_control_run.current_indicator = 'Y'
	  AND wbmi_audit_control_run.end_ts IS NULL
),
EXP_audit_control_run AS (
	SELECT
	wbmi_audit_control_run_id,
	start_ts,
	sysdate AS end_ts,
	sysdate AS modified_date
	FROM SQ_wbmi_audit_control_run_EXPIRE
),
UPD_wbmi_audit_control_run AS (
	SELECT
	wbmi_audit_control_run_id, 
	end_ts, 
	modified_date
	FROM EXP_audit_control_run
),
wbmi_audit_control_run_EXPIRE AS (
	MERGE INTO wbmi_audit_control_run AS T
	USING UPD_wbmi_audit_control_run AS S
	ON T.wbmi_audit_control_run_id = S.wbmi_audit_control_run_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.end_ts = S.end_ts, T.modified_date = S.modified_date
),