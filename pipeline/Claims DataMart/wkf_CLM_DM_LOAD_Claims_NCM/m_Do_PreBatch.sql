WITH
SQ_wbmi_batch_control_run_EXPIRE AS (
	Select batch_control_run.wbmi_batch_control_run_id
	from
	wbmi_batch_control_run as batch_control_run,
	wbmi_batch_control as batch_control
	where 
	batch_control_run.wbmi_batch_control_id = batch_control.wbmi_batch_control_id
	AND batch_control.batch_name = '@{pipeline().parameters.BATCH_NAME}'
	AND batch_control_run.current_ind='Y'
	and NOT EXISTS(SELECT 1
		FROM wbmi_batch_control_run as batch_control_run2
		WHERE batch_control_run.wbmi_batch_control_id = batch_control_run2.wbmi_batch_control_id
	       AND batch_control.batch_name = '@{pipeline().parameters.BATCH_NAME}'
	       AND batch_control_run.current_ind='Y'
		AND batch_control_run2.end_ts IS NULL)
	
	--Subquery in place to ensure that if we have not ran m_Do_PostBatch for a Batch that m_Do_PreBatch
	--does not run initialize a new set of rows in the corresponding run tables.  It will however rewrite the parm file.
),
EXP_wbmi_batch_control_imitialize_source AS (
	SELECT
	wbmi_batch_control_run_id,
	'N' AS current_ind,
	sysdate AS modified_date
	FROM SQ_wbmi_batch_control_run_EXPIRE
),
UPD_wbmi_batch_control_run_initialize AS (
	SELECT
	wbmi_batch_control_run_id, 
	current_ind, 
	modified_date
	FROM EXP_wbmi_batch_control_imitialize_source
),
wbmi_batch_control_run_EXPIRE AS (
	MERGE INTO wbmi_batch_control_run AS T
	USING UPD_wbmi_batch_control_run_initialize AS S
	ON T.wbmi_batch_control_run_id = S.wbmi_batch_control_run_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.current_ind = S.current_ind, T.modified_date = S.modified_date
),
SQ_wbmi_session_control_run_EXPIRE AS (
	select
	session_control_run.wbmi_session_control_run_id
	from
	dbo.wbmi_session_control_run as session_control_run,
	dbo.wbmi_batch_control_run as batch_control_run,
	dbo.wbmi_batch_control as batch_control
	where
	session_control_run.wbmi_batch_control_run_id = batch_control_run.wbmi_batch_control_run_id
	and batch_control_run.wbmi_batch_control_id = batch_control.wbmi_batch_control_id
	and session_control_run.current_ind = 'Y'
	and batch_control.batch_name = '@{pipeline().parameters.BATCH_NAME}'
	and NOT EXISTS(SELECT 1
		FROM wbmi_batch_control_run as batch_control_run2,
			        wbmi_session_control_run as session_control_run2
		WHERE batch_control_run2.wbmi_batch_control_run_id = session_control_run2.wbmi_batch_control_run_id
		AND  batch_control_run.wbmi_batch_control_id = batch_control_run2.wbmi_batch_control_id
	       and session_control_run.current_ind = 'Y'
	       and batch_control.batch_name = '@{pipeline().parameters.BATCH_NAME}'
		AND batch_control_run2.end_ts IS NULL)
	
	--Subquery in place to ensure that if we have not ran m_Do_PostBatch for a Batch that m_Do_PreBatch
	--does not run initialize a new set of rows in the corresponding run tables.  It will however rewrite the parm file.
),
EXP_wbmi_session_control_run_initialize AS (
	SELECT
	wbmi_session_control_run_id,
	'N' AS current_ind,
	sysdate AS modified_date
	FROM SQ_wbmi_session_control_run_EXPIRE
),
UPD_wbmi_session_control_run_initialize AS (
	SELECT
	wbmi_session_control_run_id, 
	current_ind, 
	modified_date
	FROM EXP_wbmi_session_control_run_initialize
),
wbmi_session_control_run_EXPIRE AS (
	MERGE INTO wbmi_session_control_run AS T
	USING UPD_wbmi_session_control_run_initialize AS S
	ON T.wbmi_session_control_run_id = S.wbmi_session_control_run_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.current_ind = S.current_ind, T.modified_date = S.modified_date
),
SQ_wbmi_batch_control_INITIALIZE_NEW_BATCH AS (
	select 
	batch_control.wbmi_batch_control_id,
	batch_control.batch_name,
	batch_control.email_code,
	batch_control.email_address,
	audit_control_run.wbmi_audit_control_run_id
	from
	wbmi_batch_control as batch_control,
	wbmi_audit_control_run as audit_control_run
	where audit_control_run.wbmi_audit_control_id = batch_control.wbmi_audit_control_id 
	and batch_control.batch_name= '@{pipeline().parameters.BATCH_NAME}'
	and audit_control_run.current_indicator='Y'
	and NOT EXISTS(SELECT 1
		FROM wbmi_batch_control_run as batch_control_run2
		WHERE batch_control.wbmi_batch_control_id = batch_control_run2.wbmi_batch_control_id
	      and batch_control.batch_name= '@{pipeline().parameters.BATCH_NAME}'
	      and audit_control_run.current_indicator='Y'
		AND batch_control_run2.end_ts IS NULL)
	
	--Subquery in place to ensure that if we have not ran m_Do_PostBatch for a Batch that m_Do_PreBatch
	--does not run initialize a new set of rows in the corresponding run tables.  It will however rewrite the parm file.
),
EXP_wbmi_batch_control_run AS (
	SELECT
	wbmi_batch_control_id,
	'Y' AS current_ind,
	sysdate AS start_ts,
	batch_name,
	email_code,
	email_address,
	'InformS' AS created_user_id,
	sysdate AS created_date,
	'InformS' AS modified_user_id,
	sysdate AS modified_date,
	wbmi_audit_control_run_id
	FROM SQ_wbmi_batch_control_INITIALIZE_NEW_BATCH
),
UPD_wbmi_batch_control_run AS (
	SELECT
	wbmi_batch_control_id, 
	current_ind, 
	start_ts, 
	batch_name, 
	email_code, 
	email_address, 
	created_user_id, 
	created_date, 
	modified_user_id, 
	modified_date, 
	wbmi_audit_control_run_id
	FROM EXP_wbmi_batch_control_run
),
wbmi_batch_control_run_INITIALIZE_NEW_BATCH AS (
	INSERT INTO wbmi_batch_control_run
	(wbmi_batch_control_id, batch_name, email_code, current_ind, start_ts, created_user_id, email_address, created_date, modified_user_id, modified_date, wbmi_audit_control_run_id)
	SELECT 
	WBMI_BATCH_CONTROL_ID, 
	BATCH_NAME, 
	EMAIL_CODE, 
	CURRENT_IND, 
	START_TS, 
	CREATED_USER_ID, 
	EMAIL_ADDRESS, 
	CREATED_DATE, 
	MODIFIED_USER_ID, 
	MODIFIED_DATE, 
	WBMI_AUDIT_CONTROL_RUN_ID
	FROM UPD_wbmi_batch_control_run
),
SQ_wbmi_session_control_INCREMENT_SEL_START_END_TS AS (
	SELECT 
	batch_control_run.batch_name, 
	session_control.wbmi_session_control_id, 
	session_control.session_name, 
	session_control.selection_start_ts, 
	session_control.selection_end_ts, 
	session_control.selection_incrementation_pattern, 
	batch_control_run.end_ts 
	FROM 
	wbmi_session_control as session_control, 
	wbmi_batch_control_run as batch_control_run
	WHERE session_control.wbmi_batch_control_id = batch_control_run.wbmi_batch_control_id
	AND batch_control_run.batch_name = '@{pipeline().parameters.BATCH_NAME}'
	AND batch_control_run.current_ind='Y' 
	and NOT EXISTS(SELECT 1
		FROM wbmi_batch_control_run as batch_control_run2,
			        wbmi_session_control_run as session_control_run2
		WHERE batch_control_run2.wbmi_batch_control_run_id = session_control_run2.wbmi_batch_control_run_id
		AND  batch_control_run.wbmi_batch_control_id = batch_control_run2.wbmi_batch_control_id
	      AND batch_control_run2.batch_name = '@{pipeline().parameters.BATCH_NAME}'
	      AND batch_control_run2.current_ind='Y'  
		AND batch_control_run2.end_ts IS NULL)
	
	--Subquery in place to ensure that if we have not ran m_Do_PostBatch for a Batch that m_Do_PreBatch
	--does not run initialize a new set of rows in the corresponding run tables.  It will however rewrite the parm file.
),
EXP_wbmi_session_control AS (
	SELECT
	batch_name AS bacth_name,
	wbmi_session_control_id,
	session_name,
	selection_start_ts AS in_selection_start_ts,
	selection_end_ts AS in_selection_end_ts,
	end_ts,
	selection_incrementation_pattern AS in_selection_incrementation_pattern,
	-- *INF*: ADD_TO_DATE(in_selection_end_ts,'SS', 1)
	-- 
	-- 
	-- 
	ADD_TO_DATE(in_selection_end_ts, 'SS', 1) AS out_selection_start_ts,
	CURRENT_TIMESTAMP AS out_selection_end_ts
	FROM SQ_wbmi_session_control_INCREMENT_SEL_START_END_TS
),
UPD_wbmi_session_control AS (
	SELECT
	wbmi_session_control_id, 
	out_selection_start_ts AS selection_start_ts, 
	out_selection_end_ts AS selection_end_ts
	FROM EXP_wbmi_session_control
),
wbmi_session_control_INCREMENT_SEL_START_END_TS AS (
	MERGE INTO wbmi_session_control AS T
	USING UPD_wbmi_session_control AS S
	ON T.wbmi_session_control_id = S.wbmi_session_control_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.selection_start_ts = S.selection_start_ts, T.selection_end_ts = S.selection_end_ts
),
SQ_wbmi_session_control_INITIALIZE AS (
	SELECT 
	session_control.wbmi_session_control_id, 
	session_control.folder_name,
	session_control.workflow_name,
	session_control.session_name,
	session_control.selection_start_ts,
	session_control.selection_end_ts,
	session_control.selection_incrementation_pattern,
	session_control.source_database_connection,
	session_control.target_database_connection,
	session_control.source_table_owner,
	session_control.target_table_owner,
	session_control.additional_parameters,
	batch_control_run.wbmi_batch_control_run_id
	FROM
	dbo.wbmi_batch_control_run as batch_control_run, 
	dbo.wbmi_session_control as session_control,
	dbo.wbmi_batch_control as batch_control
	WHERE 
	session_control.wbmi_batch_control_id = batch_control_run.wbmi_batch_control_id
	AND batch_control_run.wbmi_batch_control_id = batch_control.wbmi_batch_control_id
	AND batch_control.batch_name = '@{pipeline().parameters.BATCH_NAME}'
	AND batch_control_run.current_ind='Y'
	and NOT EXISTS(SELECT 1
		FROM wbmi_batch_control_run as batch_control_run2,
			        wbmi_session_control_run as session_control_run2
		WHERE batch_control_run2.wbmi_batch_control_run_id = session_control_run2.wbmi_batch_control_run_id
		AND  batch_control_run.wbmi_batch_control_id = batch_control_run2.wbmi_batch_control_id
		AND batch_control_run2.end_ts IS NULL)
	
	--Subquery in place to ensure that if we have not ran m_Do_PostBatch for a Batch that m_Do_PreBatch
	--does not run initialize a new set of rows in the corresponding run tables.  It will however rewrite the parm file.
),
EXP_session_control AS (
	SELECT
	wbmi_session_control_id,
	'Y' AS current_ind,
	'InformS ' AS created_user_id,
	sysdate AS created_date,
	'InformS' AS modified_user_id,
	sysdate AS modified_date
	FROM SQ_wbmi_session_control_INITIALIZE
),
UPD_wbmi_session_control_run AS (
	SELECT
	SQ_wbmi_session_control_INITIALIZE.wbmi_session_control_id, 
	SQ_wbmi_session_control_INITIALIZE.wbmi_batch_control_run_id, 
	SQ_wbmi_session_control_INITIALIZE.folder_name, 
	SQ_wbmi_session_control_INITIALIZE.workflow_name, 
	SQ_wbmi_session_control_INITIALIZE.session_name, 
	SQ_wbmi_session_control_INITIALIZE.selection_start_ts, 
	SQ_wbmi_session_control_INITIALIZE.selection_end_ts, 
	SQ_wbmi_session_control_INITIALIZE.selection_incrementation_pattern, 
	SQ_wbmi_session_control_INITIALIZE.source_database_connection, 
	SQ_wbmi_session_control_INITIALIZE.target_database_connection, 
	SQ_wbmi_session_control_INITIALIZE.source_table_owner, 
	SQ_wbmi_session_control_INITIALIZE.target_table_owner, 
	SQ_wbmi_session_control_INITIALIZE.additional_parameters, 
	EXP_session_control.current_ind, 
	EXP_session_control.created_user_id, 
	EXP_session_control.created_date, 
	EXP_session_control.modified_user_id, 
	EXP_session_control.modified_date
	FROM EXP_session_control
	 -- Manually join with SQ_wbmi_session_control_INITIALIZE
),
wbmi_session_control_run_INITIALIZE_SESSIONS AS (
	INSERT INTO wbmi_session_control_run
	(wbmi_session_control_id, wbmi_batch_control_run_id, folder_name, workflow_name, session_name, selection_start_ts, selection_end_ts, selection_incrementation_pattern, source_database_connection, target_database_connection, source_table_owner, target_table_owner, additional_parameters, current_ind, created_user_id, created_date, modified_user_id, modified_date)
	SELECT 
	WBMI_SESSION_CONTROL_ID, 
	WBMI_BATCH_CONTROL_RUN_ID, 
	FOLDER_NAME, 
	WORKFLOW_NAME, 
	SESSION_NAME, 
	SELECTION_START_TS, 
	SELECTION_END_TS, 
	SELECTION_INCREMENTATION_PATTERN, 
	SOURCE_DATABASE_CONNECTION, 
	TARGET_DATABASE_CONNECTION, 
	SOURCE_TABLE_OWNER, 
	TARGET_TABLE_OWNER, 
	ADDITIONAL_PARAMETERS, 
	CURRENT_IND, 
	CREATED_USER_ID, 
	CREATED_DATE, 
	MODIFIED_USER_ID, 
	MODIFIED_DATE
	FROM UPD_wbmi_session_control_run
),
SQ_wbmi_session_control_run_PARM_FILE AS (
	select
	batch_control_run.wbmi_audit_control_run_id,
	session_control_run.wbmi_session_control_run_id,
	session_control_run.wbmi_batch_control_run_id,
	session_control_run.folder_name,
	session_control_run.workflow_name,
	session_control_run.session_name,
	session_control_run.selection_start_ts,
	session_control_run.selection_end_ts,
	session_control_run.source_database_connection,
	session_control_run.target_database_connection,
	session_control_run.source_table_owner,
	session_control_run.target_table_owner,
	session_control_run.additional_parameters
	from 
	dbo.wbmi_session_control_run as session_control_run,
	dbo.wbmi_batch_control_run as batch_control_run
	where
	batch_control_run.wbmi_batch_control_run_id = session_control_run.wbmi_batch_control_run_id 
	and batch_control_run.batch_name='@{pipeline().parameters.BATCH_NAME}'
	and session_control_run.current_ind = 'Y'
),
EXP_out_Parm_file AS (
	SELECT
	wbmi_audit_control_run_id,
	wbmi_session_control_run_id,
	wbmi_batch_control_run_id,
	folder_name,
	workflow_name,
	session_name,
	selection_start_ts,
	selection_end_ts,
	source_database_connection,
	target_database_connection,
	source_table_owner,
	target_table_owner,
	additional_parameters,
	-- *INF*: '[' || RTRIM(folder_name) || '.' ||
	--   RTRIM(session_name) || ']' || CHR(10) ||
	-- '@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}=' ||
	--  TO_CHAR (wbmi_audit_control_run_id )  || CHR(10) ||
	-- '@{pipeline().parameters.WBMI_BATCH_CONTROL_RUN_ID}=' ||
	--  TO_CHAR (wbmi_batch_control_run_id )  || CHR(10) ||
	-- '@{pipeline().parameters.WBMI_SESSION_CONTROL_RUN_ID}=' || 
	-- TO_CHAR (wbmi_session_control_run_id )  ||  CHR(10) ||
	-- '@{pipeline().parameters.SELECTION_START_TS}=' || TO_CHAR( selection_start_ts)  || CHR(10) ||
	-- '@{pipeline().parameters.SELECTION_END_TS}=' || TO_CHAR( selection_end_ts)  ||  CHR(10) ||
	-- '@{pipeline().parameters.DBCONNECTION_SOURCE}=' ||  RTRIM(source_database_connection) || CHR(10) ||
	-- '@{pipeline().parameters.DBCONNECTION_TARGET}=' ||  RTRIM(target_database_connection) || CHR(10) ||
	-- '@{pipeline().parameters.SOURCE_TABLE_OWNER}=' ||  RTRIM(source_table_owner) || CHR(10) ||
	-- '@{pipeline().parameters.TARGET_TABLE_OWNER}=' ||  RTRIM(target_table_owner) || CHR(10) ||
	-- IIF(ISNULL(additional_parameters), '', RTRIM(additional_parameters))  ||  CHR(10) 
	'[' || RTRIM(folder_name) || '.' || RTRIM(session_name) || ']' || CHR(10) || '@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}=' || TO_CHAR(wbmi_audit_control_run_id) || CHR(10) || '@{pipeline().parameters.WBMI_BATCH_CONTROL_RUN_ID}=' || TO_CHAR(wbmi_batch_control_run_id) || CHR(10) || '@{pipeline().parameters.WBMI_SESSION_CONTROL_RUN_ID}=' || TO_CHAR(wbmi_session_control_run_id) || CHR(10) || '@{pipeline().parameters.SELECTION_START_TS}=' || TO_CHAR(selection_start_ts) || CHR(10) || '@{pipeline().parameters.SELECTION_END_TS}=' || TO_CHAR(selection_end_ts) || CHR(10) || '@{pipeline().parameters.DBCONNECTION_SOURCE}=' || RTRIM(source_database_connection) || CHR(10) || '@{pipeline().parameters.DBCONNECTION_TARGET}=' || RTRIM(target_database_connection) || CHR(10) || '@{pipeline().parameters.SOURCE_TABLE_OWNER}=' || RTRIM(source_table_owner) || CHR(10) || '@{pipeline().parameters.TARGET_TABLE_OWNER}=' || RTRIM(target_table_owner) || CHR(10) || IFF(additional_parameters IS NULL, '', RTRIM(additional_parameters)) || CHR(10) AS out_Param_Data
	FROM SQ_wbmi_session_control_run_PARM_FILE
),
Batch_Name AS (
	INSERT INTO Batch_Name_Parm
	(Parm_Data1)
	SELECT 
	out_Param_Data AS PARM_DATA1
	FROM EXP_out_Parm_file
),