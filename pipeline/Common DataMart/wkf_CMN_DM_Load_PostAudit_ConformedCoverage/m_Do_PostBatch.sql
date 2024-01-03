WITH
SQ_wbmi_session_control_run_EXPIRE AS (
	select
	session_control_run.wbmi_session_control_run_id,
	session_control_run.workflow_name,
	session_control_run.session_name
	from
	dbo.wbmi_session_control_run as session_control_run,
	dbo.wbmi_batch_control_run as batch_control_run
	where
	session_control_run.wbmi_batch_control_run_id = batch_control_run.wbmi_batch_control_run_id
	and batch_control_run.batch_name = '@{pipeline().parameters.BATCH_NAME}'
	and session_control_run.current_ind = 'Y'
	and batch_control_run.end_ts IS NULL
),
EXP_session_control_run AS (
	SELECT
	wbmi_session_control_run_id,
	sysdate AS modified_date,
	workflow_name,
	session_name
	FROM SQ_wbmi_session_control_run_EXPIRE
),
LKP_REP_SESS_LOG AS (
	SELECT
	workflow_run_id,
	sess_inst_id,
	workflow_name,
	session_name
	FROM (
		SELECT 
		MAX(WORKFLOW_RUN_ID) as workflow_run_id, 
		MAX(INSTANCE_ID) as sess_inst_id, 
		WORKFLOW_NAME as workflow_name,
		SESSION_NAME as session_name 
		FROM  PowerCenter.REP_SESS_LOG
		WHERE SUBJECT_AREA like '%@{pipeline().parameters.SUBJECT_AREA}%'
		GROUP BY SESSION_NAME ,WORKFLOW_NAME
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY workflow_name,session_name ORDER BY workflow_run_id) = 1
),
UPD_session_control_run AS (
	SELECT
	EXP_session_control_run.wbmi_session_control_run_id, 
	EXP_session_control_run.modified_date, 
	LKP_REP_SESS_LOG.workflow_run_id, 
	LKP_REP_SESS_LOG.sess_inst_id
	FROM EXP_session_control_run
	LEFT JOIN LKP_REP_SESS_LOG
	ON LKP_REP_SESS_LOG.workflow_name = EXP_session_control_run.workflow_name AND LKP_REP_SESS_LOG.session_name = EXP_session_control_run.session_name
),
wbmi_session_control_run_EXPIRE AS (
	MERGE INTO wbmi_session_control_run AS T
	USING UPD_session_control_run AS S
	ON T.wbmi_session_control_run_id = S.wbmi_session_control_run_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.workflow_run_id = S.workflow_run_id, T.sess_inst_id = S.sess_inst_id, T.modified_date = S.modified_date
),
SQ_wbmi_batch_control_run_EXPIRE AS (
	select 
	batch_control_run.wbmi_batch_control_run_id,
	batch_control_run.start_ts
	from
	dbo.wbmi_batch_control_run as batch_control_run,
	dbo.wbmi_batch_control as batch_control
	where 
	batch_control.wbmi_batch_control_id = batch_control_run.wbmi_batch_control_id
	and batch_control.batch_name = '@{pipeline().parameters.BATCH_NAME}'
	and current_ind = 'Y'
	and end_ts IS NULL
),
EXP_batch_control_run AS (
	SELECT
	wbmi_batch_control_run_id,
	start_ts,
	CURRENT_TIMESTAMP AS last_modified_ts,
	CURRENT_TIMESTAMP AS end_ts,
	-- *INF*: ADD_TO_DATE(
	-- 	TO_DATE('01/01/1800','MM/DD/YYYY'),
	-- 	'SS',
	-- 	DATE_DIFF(sysdate,start_ts,'SS'))
	ADD_TO_DATE(TO_DATE('01/01/1800', 'MM/DD/YYYY'), 'SS', DATE_DIFF(sysdate, start_ts, 'SS')) AS elapsed_time,
	sysdate AS modified_date
	FROM SQ_wbmi_batch_control_run_EXPIRE
),
UPD_batch_control_run AS (
	SELECT
	wbmi_batch_control_run_id, 
	end_ts, 
	elapsed_time, 
	modified_date
	FROM EXP_batch_control_run
),
wbmi_batch_control_run_EXPIRE AS (
	MERGE INTO wbmi_batch_control_run AS T
	USING UPD_batch_control_run AS S
	ON T.wbmi_batch_control_run_id = S.wbmi_batch_control_run_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.end_ts = S.end_ts, T.elapsed_time = S.elapsed_time, T.modified_date = S.modified_date
),