WITH
SQ_wbmi_checkout AS (
	SELECT DISTINCT  '@{pipeline().parameters.DEFAULT_EMAIL_BODY}' + ' <BR> <BR> ',
	'@{pipeline().parameters.EMAIL_ADDRESS}'
	FROM
	dbo.wbmi_session_control_run wbmi_session_control_run,
	dbo.wbmi_batch_control_run wbmi_batch_control_run
	WHERE
	wbmi_session_control_run.current_ind = 'Y'  and 
	wbmi_session_control_run.wbmi_batch_control_run_id = wbmi_batch_control_run.wbmi_batch_control_run_id and
	wbmi_batch_control_run.batch_name = '@{pipeline().parameters.BATCH_NAME}'
	
	UNION ALL
	
	select distinct 
	wbmi_checkout.checkout_message + ' <BR> <BR> ',
	'@{pipeline().parameters.EMAIL_ADDRESS}'
	from 
	dbo.wbmi_checkout wbmi_checkout,
	dbo.wbmi_session_control_run wbmi_session_control_run,
	dbo.wbmi_batch_control_run wbmi_batch_control_run
	where
	wbmi_checkout.checkout_type_code in ('C') and 
	wbmi_checkout.wbmi_session_control_run_id = wbmi_session_control_run.wbmi_session_control_run_id and
	wbmi_session_control_run.current_ind = 'Y'  and 
	wbmi_session_control_run.wbmi_batch_control_run_id = wbmi_batch_control_run.wbmi_batch_control_run_id and
	wbmi_batch_control_run.batch_name  in ('@{pipeline().parameters.BATCH_NAME}','CLAIMS_DATAMART')
),
EXP_Email_Subject AS (
	SELECT
	email_address,
	checkout_message,
	-- *INF*: @{pipeline().parameters.EMAIL_SUBJECT} || ' (' || SYSDATE || ')'
	-- 
	@{pipeline().parameters.EMAIL_SUBJECT} || ' (' || SYSDATE || ')' AS email_subject
	FROM SQ_wbmi_checkout
),
AGG_Distinct_Email_Id AS (
	SELECT
	email_address, 
	email_subject
	FROM EXP_Email_Subject
	QUALIFY ROW_NUMBER() OVER (PARTITION BY email_address, email_subject ORDER BY NULL) = 1
),
email_subject AS (
	INSERT INTO email_subject
	(FIELD1)
	SELECT 
	email_subject AS FIELD1
	FROM AGG_Distinct_Email_Id
),
email_body AS (
	INSERT INTO email_body
	(FIELD1)
	SELECT 
	checkout_message AS FIELD1
	FROM EXP_Email_Subject
),
email_address AS (
	INSERT INTO email_address
	(FIELD1)
	SELECT 
	email_address AS FIELD1
	FROM AGG_Distinct_Email_Id
),