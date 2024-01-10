WITH
SQ_loss_master_calculation AS (
	SELECT COUNT(*) AS EDW_Count, 'Count of claim_occurrence_ak_id from Loss_Master_Calculation where claim_occurrence_ak_id = - 1 is ' + CONVERT(varchar,COUNT(*)) as Check_Out_Message,'Loss_Master_Calculation' as Target_Name FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.Loss_Master_Calculation where claim_occurrence_ak_id = - 1
	
	UNION
	
	SELECT COUNT(*) AS EDW_Count, 'Count of pol_ak_id from Loss_Master_Calculation where pol_ak_id = - 1 is ' + CONVERT(varchar,COUNT(*)) as Check_Out_Message,'Loss_Master_Calculation' as Target_Name FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.Loss_Master_Calculation where pol_ak_id = - 1
	
	UNION
	
	SELECT COUNT(*) AS EDW_Count, 'Count of claim_party_ak_id from Loss_Master_Calculation where claim_party_ak_id = - 1 is ' + CONVERT(varchar,COUNT(*)) as Check_Out_Message,'Loss_Master_Calculation' as Target_Name FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.Loss_Master_Calculation where claim_party_ak_id = - 1
	
	UNION
	
	SELECT COUNT(*) AS EDW_Count, 'Count of claimant_cov_det_ak_id from Loss_Master_Calculation where claimant_cov_det_ak_id = - 1 is ' + CONVERT(varchar,COUNT(*)) as Check_Out_Message,'Loss_Master_Calculation' as Target_Name FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.Loss_Master_Calculation where claimant_cov_det_ak_id = - 1
	
	UNION
	
	SELECT COUNT(*) AS EDW_Count, 'Count of claim_party_occurrence_ak_id from Loss_Master_Calculation where claim_party_occurrence_ak_id = - 1 is ' + CONVERT(varchar,COUNT(*)) as Check_Out_Message,'Loss_Master_Calculation' as Target_Name FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.Loss_Master_Calculation where claim_party_occurrence_ak_id = - 1
	
	
	--UNION
	
	--SELECT COUNT(*) AS EDW_Count, 'Count of claim_trans_ak_id from  Loss_Master_Calculation where claim_trans_ak_id = - 1 is ' + CONVERT(varchar,COUNT(*)) as check_Out_Message,'Loss_Master_Calculation' as Target_Name FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.Loss_Master_Calculation where claim_trans_ak_id = - 1
	
	--UNION
	
	--SELECT COUNT(*) AS EDW_Count, 'Count of claim_reins_trans_ak_id from  Loss_Master_Calculation where claim_reins_trans_ak_id = - 1 is ' + CONVERT(varchar,COUNT(*)) as Check_Out_Message,'Loss_Master_Calculation' as Target_Name FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.Loss_Master_Calculation where claim_reins_trans_ak_id = - 1
	
	--UNION
	
	--SELECT COUNT(*) AS EDW_Count, 'Count of agency_ak_id from Loss_Master_Calculation where agency_ak_id = - 1 is ' + CONVERT(varchar,COUNT(*)) as Check_Out_Message,'Loss_Master_Calculation' as Target_Name FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.Loss_Master_Calculation where agency_ak_id = - 1
	
	--UNION
	
	--SELECT COUNT(*) AS EDW_Count, 'Count of contract_cust_ak_id from Loss_Master_Calculation where contract_cust_ak_id = - 1 is ' + CONVERT(varchar,COUNT(*)) as Check_Out_Message,'Loss_Master_Calculation' as Target_Name FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.Loss_Master_Calculation where contract_cust_ak_id = - 1
	
	--UNION
	
	--SELECT COUNT(*) AS EDW_Count, 'Count of claim_primary_rep_ak_id from Loss_Master_Calculation where claim_primary_rep_ak_id = - 1 is ' + CONVERT(varchar,COUNT(*)) as Check_Out_Message,'Loss_Master_Calculation' as Target_Name FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.Loss_Master_Calculation where claim_primary_rep_ak_id = - 1
	
	--UNION
	
	--SELECT COUNT(*) AS EDW_Count, 'Count of claim_examiner_ak_id from Loss_Master_Calculation where claim_examiner_ak_id = - 1 is ' + CONVERT(varchar,COUNT(*)) as Check_Out_Message,'Loss_Master_Calculation' as Target_Name FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.Loss_Master_Calculation where claim_examiner_ak_id = - 1
	
	--UNION
	
	--SELECT COUNT(*) AS EDW_Count, 'Count of claim_case_ak_id from Loss_Master_Calculation where claim_case_ak_id = - 1 is ' + CONVERT(varchar,COUNT(*)) as Check_Out_Message,'Loss_Master_Calculation' as Target_Name FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.Loss_Master_Calculation where claim_case_ak_id = - 1
	
	--UNION
	
	--SELECT COUNT(*) AS EDW_Count, 'Count of wc_claimant_det_ak_id from Loss_Master_Calculation where wc_claimant_det_ak_id = - 1 is ' + CONVERT(varchar,COUNT(*)) as Check_Out_Message,'Loss_Master_Calculation' as Target_Name FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.Loss_Master_Calculation where wc_claimant_det_ak_id = - 1
	
	--UNION
	
	--SELECT COUNT(*) AS EDW_Count, 'Count of claim_pay_ak_id from Loss_Master_Calculation where claim_pay_ak_id = - 1 is ' + CONVERT(varchar,COUNT(*)) as Check_Out_Message,'Loss_Master_Calculation' as Target_Name FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.Loss_Master_Calculation where claim_pay_ak_id = - 1
	
	--UNION
	
	--SELECT COUNT(*) AS EDW_Count, 'Count of reins_cov_ak_id from Loss_Master_Calculation where reins_cov_ak_id = - 1 is ' + CONVERT(varchar,COUNT(*)) as Check_Out_Message,'Loss_Master_Calculation' as Target_Name  FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.Loss_Master_Calculation where reins_cov_ak_id = - 1
),
EXP_default AS (
	SELECT
	EDW_Count AS EDW_count,
	Check_Out_Message AS check_out_message,
	@{pipeline().parameters.WBMI_SESSION_CONTROL_RUN_ID} AS wbmi_session_control_run_id,
	'InformS' AS created_user_id,
	SYSDATE AS created_date,
	'InformS' AS modified_user_id,
	SYSDATE AS modified_date,
	Target_Name AS target_name,
	-- *INF*: 'E'
	-- 
	-- //E - Error, W - Warning
	'E' AS checkout_type_code
	FROM SQ_loss_master_calculation
),
FILTRANS AS (
	SELECT
	EDW_count, 
	check_out_message, 
	wbmi_session_control_run_id, 
	created_user_id, 
	created_date, 
	modified_user_id, 
	modified_date, 
	target_name, 
	checkout_type_code
	FROM EXP_default
	WHERE EDW_count>0
),
wbmi_checkout_curr_row_count AS (
	INSERT INTO wbmi_checkout
	(wbmi_session_control_run_id, checkout_type_code, checkout_message, target_name, target_count, created_user_id, created_date, modified_user_id, modified_date)
	SELECT 
	WBMI_SESSION_CONTROL_RUN_ID, 
	CHECKOUT_TYPE_CODE, 
	check_out_message AS CHECKOUT_MESSAGE, 
	TARGET_NAME, 
	EDW_count AS TARGET_COUNT, 
	CREATED_USER_ID, 
	CREATED_DATE, 
	MODIFIED_USER_ID, 
	MODIFIED_DATE
	FROM FILTRANS
),
SQ_loss_master_calculation_high_threshold AS (
	SELECT COUNT(*) AS EDW_Count, 'Count of cov_ak_id from Loss_Master_Calculation where cov_ak_id = - 1 is ' + CONVERT(varchar,COUNT(*)) as Check_Out_Message,'Loss_Master_Calculation' as Target_Name FROM Loss_Master_Calculation LMC, dbo.claimant_coverage_detail CCD where LMC.cov_ak_id = - 1 and LMC.claimant_cov_det_ak_id = CCD.claimant_cov_det_ak_id AND 
	CCD.crrnt_snpsht_flag = 1 AND CCD.major_peril_code <> '101'
	AND CCD.PolicySourceID NOT IN ('PDC','DUC') --- For Claims of DuckCreek Policies will not have Cov_AK_ID values
	AND LMC.created_date >='@{pipeline().parameters.SELECTION_START_TS}'
	
	UNION
	
	SELECT COUNT(*) AS EDW_Count, 'Count of temp_pol_trans_ak_id from Loss_Master_Calculation where temp_pol_trans_ak_id = - 1 is ' + CONVERT(varchar,COUNT(*)) as Check_Out_Message,'Loss_Master_Calculation' as Target_Name FROM Loss_Master_Calculation LMC,dbo.claimant_coverage_detail CCD where LMC.temp_pol_trans_ak_id = - 1 and LMC.claimant_cov_det_ak_id = CCD.claimant_cov_det_ak_id 
	AND CCD.crrnt_snpsht_flag = 1 AND CCD.major_peril_code <> '101'
	AND CCD.PolicySourceID NOT IN ('PDC','DUC') --- For Claims of DuckCreek Policies will not have Temp_Pol_Trans_AK_ID values
	AND LMC.created_date >='@{pipeline().parameters.SELECTION_START_TS}'
),
EXP_default_high_threshold AS (
	SELECT
	EDW_Count AS EDW_count,
	Check_Out_Message AS check_out_message,
	@{pipeline().parameters.WBMI_SESSION_CONTROL_RUN_ID} AS wbmi_session_control_run_id,
	'InformS' AS created_user_id,
	SYSDATE AS created_date,
	'InformS' AS modified_user_id,
	SYSDATE AS modified_date,
	Target_Name AS target_name,
	-- *INF*: 'E'
	-- 
	-- //E - Error, W - Warning
	'E' AS checkout_type_code
	FROM SQ_loss_master_calculation_high_threshold
),
FILTRANS_high_threshold AS (
	SELECT
	EDW_count, 
	check_out_message, 
	wbmi_session_control_run_id, 
	created_user_id, 
	created_date, 
	modified_user_id, 
	modified_date, 
	target_name, 
	checkout_type_code
	FROM EXP_default_high_threshold
	WHERE EDW_count > 0
),
wbmi_checkout_curr_row_count_high_threshold AS (
	INSERT INTO wbmi_checkout
	(wbmi_session_control_run_id, checkout_type_code, checkout_message, target_name, target_count, created_user_id, created_date, modified_user_id, modified_date)
	SELECT 
	WBMI_SESSION_CONTROL_RUN_ID, 
	CHECKOUT_TYPE_CODE, 
	check_out_message AS CHECKOUT_MESSAGE, 
	TARGET_NAME, 
	EDW_count AS TARGET_COUNT, 
	CREATED_USER_ID, 
	CREATED_DATE, 
	MODIFIED_USER_ID, 
	MODIFIED_DATE
	FROM FILTRANS_high_threshold
),
SQ_wbmi_checkout AS (
	select 
	wbmi_checkout.checkout_message + ' <BR> <BR> ',
	wbmi_batch_control_run.email_address
	from 
	dbo.wbmi_checkout wbmi_checkout,
	dbo.wbmi_session_control_run wbmi_session_control_run,
	dbo.wbmi_batch_control_run wbmi_batch_control_run
	where
	wbmi_checkout.checkout_type_code = 'E' and 
	wbmi_checkout.wbmi_session_control_run_id = wbmi_session_control_run.wbmi_session_control_run_id and
	wbmi_session_control_run.current_ind = 'Y'  and 
	wbmi_session_control_run.wbmi_batch_control_run_id = wbmi_batch_control_run.wbmi_batch_control_run_id and
	rtrim(wbmi_batch_control_run.batch_name) = 'LOSS_MASTER_CALCULATION'
	order by wbmi_checkout_id
),
EXP_Email_Subject AS (
	SELECT
	email_address,
	checkout_message,
	-- *INF*: 'There are errors in the Loss_Master_Calculation EDW data. Execution aborted (' || sysdate || ')'
	'There are errors in the Loss_Master_Calculation EDW data. Execution aborted (' || sysdate || ')' AS email_subject
	FROM SQ_wbmi_checkout
),
AGG_Distinct_Email_Id AS (
	SELECT
	email_address,
	email_subject
	FROM EXP_Email_Subject
	QUALIFY ROW_NUMBER() OVER (PARTITION BY email_address, email_subject ORDER BY NULL) = 1
),
email_address AS (
	INSERT INTO email_address
	(FIELD1)
	SELECT 
	email_address AS FIELD1
	FROM AGG_Distinct_Email_Id
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
SQ_wbmi_checkout1 AS (
	select 
	wbmi_checkout.checkout_message + ' <BR> <BR> ',
	wbmi_batch_control_run.email_address
	from 
	dbo.wbmi_checkout wbmi_checkout,
	dbo.wbmi_session_control_run wbmi_session_control_run,
	dbo.wbmi_batch_control_run wbmi_batch_control_run
	where
	wbmi_checkout.checkout_type_code = 'E' and 
	wbmi_checkout.wbmi_session_control_run_id = wbmi_session_control_run.wbmi_session_control_run_id and
	wbmi_session_control_run.current_ind = 'Y'  and 
	wbmi_session_control_run.wbmi_batch_control_run_id = wbmi_batch_control_run.wbmi_batch_control_run_id and
	rtrim(wbmi_batch_control_run.batch_name)  = 'LOSS_MASTER_CALCULATION'
	order by wbmi_checkout_id
),
EXP_Email_Subject1 AS (
	SELECT
	email_address,
	checkout_message,
	-- *INF*: Abort('There are issues with the Loss Master Calc EDW data')
	Abort('There are issues with the Loss Master Calc EDW data'
	) AS error
	FROM SQ_wbmi_checkout1
),
FIL_STOP_PROCESSING AS (
	SELECT
	checkout_message, 
	error
	FROM EXP_Email_Subject1
	WHERE FALSE
),
wbmi_checkout_dummy_target AS (
	INSERT INTO wbmi_checkout
	(checkout_message)
	SELECT 
	CHECKOUT_MESSAGE
	FROM FIL_STOP_PROCESSING
),