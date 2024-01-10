WITH
SQ_wbmi_checkout_loss_master AS (
	SELECT 'Count of loss_master_fact_id from Loss_Master_Fact with claimant_cov_dim_id as -1 = ' + convert(varchar,count(*)) as check_out_message,'Loss_Master_Fact' as target_name, count(*) as target_count
	FROM (select loss_master_fact_id from Loss_Master_Fact where claimant_cov_dim_id = -1 )LMF
	
	UNION
	
	SELECT 'Count of loss_master_fact_id from Loss_Master_Fact with claimant_dim_id as -1 = ' + convert(varchar,count(*)) as check_out_message,'Loss_Master_Fact' as target_name, count(*) as target_count
	FROM (select loss_master_fact_id from Loss_Master_Fact where claimant_dim_id = -1 )LMF
	
	UNION
	
	SELECT 'Count of loss_master_fact_id from Loss_Master_Fact with claim_occurrence_dim_id as -1 = ' + convert(varchar,count(*)) as check_out_message,'Loss_Master_Fact' as target_name, count(*) as target_count
	FROM (select loss_master_fact_id from Loss_Master_Fact where claim_occurrence_dim_id = -1 )LMF
	
	UNION
	
	SELECT 'Count of loss_master_fact_id from Loss_Master_Fact with pol_dim_id as -1 = ' + convert(varchar,count(*)) as check_out_message,'Loss_Master_Fact' as target_name, count(*) as target_count
	FROM (select loss_master_fact_id from Loss_Master_Fact where pol_dim_id = -1 )LMF
	
	UNION
	
	SELECT 'Count of loss_master_fact_id from Loss_Master_Fact with agency_dim_id as -1 = ' + convert(varchar,count(*)) as check_out_message,'Loss_Master_Fact' as target_name, count(*) as target_count
	FROM (select loss_master_fact_id from Loss_Master_Fact where agency_dim_id = -1 )LMF
	
	UNION
	
	SELECT 'Count of loss_master_fact_id from Loss_Master_Fact with claim_rep_dim_prim_claim_rep_id as -1 = ' + convert(varchar,count(*)) as check_out_message,'Loss_Master_Fact' as target_name, count(*) as target_count
	FROM (select loss_master_fact_id from Loss_Master_Fact where claim_rep_dim_prim_claim_rep_id = -1 )LMF
	
	UNION
	
	SELECT 'Count of loss_master_fact_id from Loss_Master_Fact with claim_trans_type_dim_id as -1 = ' + convert(varchar,count(*)) as check_out_message,'Loss_Master_Fact' as target_name, count(*) as target_count
	FROM (select loss_master_fact_id from Loss_Master_Fact where claim_trans_type_dim_id = -1 )LMF
	
	UNION 
	
	SELECT 'Count of loss_master_fact_id from Loss_Master_Fact with contract_cust_dim_id as -1 = ' + convert(varchar,count(*)) as check_out_message,'Loss_Master_Fact' as target_name, count(*) as target_count
	FROM (select loss_master_fact_id from Loss_Master_Fact where contract_cust_dim_id = -1 )LMF
	
	UNION
	
	SELECT 'Count of loss_master_fact_id from Loss_Master_Fact with claim_loss_date_id as -1 = ' + convert(varchar,count(*)) as check_out_message,'Loss_Master_Fact' as target_name, count(*) as target_count
	FROM (select loss_master_fact_id from Loss_Master_Fact where claim_loss_date_id = -1 )LMF
	
	UNION
	
	SELECT 'Count of loss_master_fact_id from Loss_Master_Fact with claim_trans_date_id as -1 = ' + convert(varchar,count(*)) as check_out_message,'Loss_Master_Fact' as target_name, count(*) as target_count
	FROM (select loss_master_fact_id from Loss_Master_Fact where claim_trans_date_id = -1 )LMF
	
	UNION
	
	SELECT 'Count of loss_master_fact_id from Loss_Master_Fact with pol_eff_date_id as -1 = ' + convert(varchar,count(*)) as check_out_message,'Loss_Master_Fact' as target_name, count(*) as target_count
	FROM (select loss_master_fact_id from Loss_Master_Fact where pol_eff_date_id = -1 )LMF
	
	UNION
	
	SELECT 'Count of loss_master_fact_id from Loss_Master_Fact with pol_exp_date_id as -1 = ' + convert(varchar,count(*)) as check_out_message,'Loss_Master_Fact' as target_name, count(*) as target_count
	FROM (select loss_master_fact_id from Loss_Master_Fact where pol_exp_date_id = -1 )LMF
	
	UNION
	
	SELECT 'Count of loss_master_fact_id from Loss_Master_Fact with asl_dim_id as -1 = ' + convert(varchar,count(*)) as check_out_message,'Loss_Master_Fact' as target_name, count(*) as target_count
	FROM (select loss_master_fact_id from Loss_Master_Fact where asl_dim_id = -1 )LMF
	
	UNION
	
	SELECT 'Count of loss_master_fact_id from Loss_Master_Fact with asl_prdct_code_dim_id as -1 = ' + convert(varchar,count(*)) as check_out_message,'Loss_Master_Fact' as target_name, count(*) as target_count
	FROM (select loss_master_fact_id from Loss_Master_Fact where asl_prdct_code_dim_id = -1 )LMF
	
	UNION
	
	SELECT 'Count of loss_master_fact_id from Loss_Master_Fact with loss_master_dim_id as -1 = ' + convert(varchar,count(*)) as check_out_message,'Loss_Master_Fact' as target_name, count(*) as target_count
	FROM (select loss_master_fact_id from Loss_Master_Fact where loss_master_dim_id = -1 )LMF
	
	UNION
	
	SELECT 'Count of loss_master_fact_id from Loss_Master_Fact with strtgc_bus_dvsn_dim_id as -1 = ' + convert(varchar,count(*)) as check_out_message,'Loss_Master_Fact' as target_name, count(*) as target_count
	FROM (select loss_master_fact_id from Loss_Master_Fact where strtgc_bus_dvsn_dim_id = -1 )LMF
	
	UNION
	
	SELECT 'Count of loss_master_fact_id from Loss_Master_Fact with prdct_code_dim_id as -1 = ' + convert(varchar,count(*)) as check_out_message,'Loss_Master_Fact' as target_name, count(*) as target_count
	FROM (select loss_master_fact_id from Loss_Master_Fact where prdct_code_dim_id = -1 )LMF
	
	UNION
	
	SELECT 'Count of loss_master_fact_id from Loss_Master_Fact with prdct_code_dim_id as -1 = ' + convert(varchar,count(*)) as check_out_message,'Loss_Master_Fact' as target_name, count(*) as target_count
	FROM (select loss_master_fact_id from Loss_Master_Fact where loss_master_run_date_id = -1 )LMF
),
EXP_Loss_master_input AS (
	SELECT
	checkout_message AS check_out_message,
	SYSDATE AS created_date,
	SYSDATE AS modified_date,
	target_name,
	target_count,
	@{pipeline().parameters.WBMI_SESSION_CONTROL_RUN_ID} AS WBMI_SESSION_CONTROL_RUN_ID,
	'InformS' AS User_ID,
	0 AS Default_Amt,
	'W' AS checkout_type
	FROM SQ_wbmi_checkout_loss_master
),
FIL_ZERO_COUNTS AS (
	SELECT
	check_out_message, 
	created_date, 
	modified_date, 
	target_name, 
	target_count, 
	WBMI_SESSION_CONTROL_RUN_ID, 
	User_ID, 
	Default_Amt, 
	checkout_type
	FROM EXP_Loss_master_input
	WHERE target_count > 0
),
UPD_CHECKOUT_TABLE_INS AS (
	SELECT
	check_out_message, 
	created_date, 
	modified_date, 
	target_name, 
	target_count, 
	WBMI_SESSION_CONTROL_RUN_ID, 
	User_ID, 
	Default_Amt, 
	checkout_type
	FROM FIL_ZERO_COUNTS
),
wbmi_checkout_loss_master AS (
	INSERT INTO wbmi_checkout
	(wbmi_session_control_run_id, checkout_type_code, checkout_message, target_name, target_count, created_user_id, created_date, modified_user_id, modified_date)
	SELECT 
	WBMI_SESSION_CONTROL_RUN_ID AS WBMI_SESSION_CONTROL_RUN_ID, 
	checkout_type AS CHECKOUT_TYPE_CODE, 
	check_out_message AS CHECKOUT_MESSAGE, 
	TARGET_NAME, 
	TARGET_COUNT, 
	User_ID AS CREATED_USER_ID, 
	CREATED_DATE, 
	User_ID AS MODIFIED_USER_ID, 
	MODIFIED_DATE
	FROM UPD_CHECKOUT_TABLE_INS
),
SQ_wbmi_checkout_loss_master_counts AS (
	SELECT 'Count of Total transactions rows from Loss_Master_Fact with Loss Master Run Date as ' + CONVERT(varchar,CD.clndr_date,101) +  ' = '+ convert(varchar,count(*)) as check_out_message,'Loss_Master_Fact' as target_name, count(*) as target_count
	,CD.clndr_date as source_dt
	FROM 
	loss_master_fact LMF, 
	calendar_dim CD 
	WHERE LMF.loss_master_run_date_id = CD.clndr_id and clndr_date > DATEADD(Month,-1,'@{pipeline().parameters.SELECTION_START_TS}')
	GROUP BY CD.clndr_date
	
	UNION
	
	SELECT 'Count of Direct transactions rows from Loss_Master_Fact with Loss Master Run Date as ' + CONVERT(varchar,CD.clndr_date,101) +  ' = '+ convert(varchar,count(*)) as check_out_message,'Loss_Master_Fact' as target_name, count(*) as target_count
	,CD.clndr_date as source_dt
	FROM 
	loss_master_fact LMF, 
	claim_transaction_type_dim CTTD, 
	calendar_dim CD 
	WHERE 
	CTTD.trans_kind_code='D' 
	and LMF.claim_trans_type_dim_id=CTTD.claim_trans_type_dim_id
	and LMF.loss_master_run_date_id = CD.clndr_id and clndr_date > DATEADD(Month,-1,'@{pipeline().parameters.SELECTION_START_TS}')
	GROUP BY CD.clndr_date
	
	UNION
	
	SELECT 'Count of Ceded transactions rows from Loss_Master_Fact with Loss Master Run Date as ' + CONVERT(varchar,CD.clndr_date,101) +  ' = '+ convert(varchar,count(*)) as check_out_message,'Loss_Master_Fact' as target_name, count(*) as target_count
	,CD.clndr_date as source_dt
	FROM 
	loss_master_fact LMF, 
	claim_transaction_type_dim CTTD, 
	calendar_dim CD 
	WHERE 
	CTTD.trans_kind_code='C' 
	and LMF.claim_trans_type_dim_id=CTTD.claim_trans_type_dim_id
	and LMF.loss_master_run_date_id = CD.clndr_id and CD.clndr_date > DATEADD(Month,-1,'@{pipeline().parameters.SELECTION_START_TS}')
	GROUP BY CD.clndr_date
	
	UNION
	
	SELECT 'Count of Direct transactions rows from Loss_Master_Fact with Loss Master Run Date as ' + CONVERT(varchar,CD.clndr_date,101) +  ' with MP 50 = ' +  convert(varchar,count(*))  as check_out_message,'Loss_Master_Fact' as target_name, count(*) as target_count
	,CD.clndr_date as source_dt
	FROM 
	loss_master_fact LMF, 
	claim_transaction_type_dim CTTD, 
	calendar_dim CD,
	coverage_dim COV  
	WHERE 
	CTTD.trans_kind_code='D' 
	and LMF.claim_trans_type_dim_id=CTTD.claim_trans_type_dim_id
	and LMF.cov_dim_id= COV.cov_dim_id
	and COV.major_peril_code='050'
	and LMF.loss_master_run_date_id = CD.clndr_id and clndr_date > DATEADD(Month,-1,'@{pipeline().parameters.SELECTION_START_TS}')
	GROUP BY CD.clndr_date
	
	UNION
	
	SELECT 'Count of Ceded transactions rows from Loss_Master_Fact with Loss Master Run Date as ' + CONVERT(varchar,CD.clndr_date,101) +  ' with MP 50 = ' +  convert(varchar,count(*))  as check_out_message,'Loss_Master_Fact' as target_name, count(*) as target_count
	,CD.clndr_date as source_dt
	FROM 
	loss_master_fact LMF, 
	claim_transaction_type_dim CTTD, 
	calendar_dim CD,
	coverage_dim COV
	WHERE 
	CTTD.trans_kind_code='C' 
	and LMF.claim_trans_type_dim_id=CTTD.claim_trans_type_dim_id
	and LMF.cov_dim_id= COV.cov_dim_id
	and COV.major_peril_code='050'
	and LMF.loss_master_run_date_id = CD.clndr_id and clndr_date > DATEADD(Month,-1,'@{pipeline().parameters.SELECTION_START_TS}')
	GROUP BY CD.clndr_date
	
	UNION
	
	SELECT 'Difference between Direct and Ceded transactions Loss_Master_Fact with MP 50 = ' + convert(varchar,
	(SELECT count(*) FROM 
	(SELECT loss_master_fact_id  FROM loss_master_fact LMF, claim_transaction_type_dim CTTD, calendar_dim CD, coverage_dim COV WHERE 
	LMF.claim_trans_type_dim_id=CTTD.claim_trans_type_dim_id
	and LMF.loss_master_run_date_id = CD.clndr_id and CD.clndr_date > DATEADD(Month,-1,'@{pipeline().parameters.SELECTION_START_TS}')
	and LMF.cov_dim_id= COV.cov_dim_id and major_peril_code='050'
	INTERSECT
	SELECT loss_master_fact_id  FROM loss_master_fact LMF, claim_transaction_type_dim CTTD, calendar_dim CD, coverage_dim COV WHERE 
	CTTD.trans_kind_code='C' and LMF.claim_trans_type_dim_id=CTTD.claim_trans_type_dim_id
	and LMF.loss_master_run_date_id = CD.clndr_id and CD.clndr_date > DATEADD(Month,-1,'@{pipeline().parameters.SELECTION_START_TS}')
	and LMF.cov_dim_id= COV.cov_dim_id and major_peril_code='050'
	) as I
	)
	-
	(
	SELECT count(*) FROM 
	(SELECT loss_master_fact_id  FROM loss_master_fact LMF, claim_transaction_type_dim CTTD, calendar_dim CD, coverage_dim COV WHERE 
	LMF.claim_trans_type_dim_id=CTTD.claim_trans_type_dim_id
	and LMF.loss_master_run_date_id = CD.clndr_id and CD.clndr_date > DATEADD(Month,-1,'@{pipeline().parameters.SELECTION_START_TS}')
	and LMF.cov_dim_id= COV.cov_dim_id and major_peril_code='050'
	EXCEPT
	SELECT loss_master_fact_id  FROM loss_master_fact LMF, claim_transaction_type_dim CTTD, calendar_dim CD, coverage_dim COV WHERE 
	CTTD.trans_kind_code='C' and LMF.claim_trans_type_dim_id=CTTD.claim_trans_type_dim_id
	and LMF.loss_master_run_date_id = CD.clndr_id and CD.clndr_date > DATEADD(Month,-1,'@{pipeline().parameters.SELECTION_START_TS}')
	and LMF.cov_dim_id= COV.cov_dim_id and major_peril_code='050'
	) as J
	)
	)as check_out_message
	,'Loss_Master_Fact' as target_name,
	0 as target_count,
	'1/1/1800' as source_dt
),
EXP_Loss_master_input_counts AS (
	SELECT
	checkout_message AS check_out_message,
	SYSDATE AS created_date,
	SYSDATE AS modified_date,
	target_name,
	target_count,
	source_dt,
	@{pipeline().parameters.WBMI_SESSION_CONTROL_RUN_ID} AS WBMI_SESSION_CONTROL_RUN_ID,
	'InformS' AS User_ID,
	0 AS Default_Amt,
	'C' AS checkout_type
	FROM SQ_wbmi_checkout_loss_master_counts
),
FIL_ZERO_COUNTS_counts AS (
	SELECT
	check_out_message, 
	created_date, 
	modified_date, 
	target_name, 
	target_count, 
	source_dt, 
	WBMI_SESSION_CONTROL_RUN_ID, 
	User_ID, 
	Default_Amt, 
	checkout_type
	FROM EXP_Loss_master_input_counts
	WHERE target_count > 0
),
UPD_CHECKOUT_TABLE_INS_counts AS (
	SELECT
	check_out_message, 
	created_date, 
	modified_date, 
	target_name, 
	target_count, 
	source_dt, 
	WBMI_SESSION_CONTROL_RUN_ID, 
	User_ID, 
	Default_Amt, 
	checkout_type
	FROM FIL_ZERO_COUNTS_counts
),
wbmi_checkout_loss_master_counts AS (
	INSERT INTO wbmi_checkout
	(wbmi_session_control_run_id, checkout_type_code, checkout_message, target_name, target_count, source_dt, created_user_id, created_date, modified_user_id, modified_date)
	SELECT 
	WBMI_SESSION_CONTROL_RUN_ID AS WBMI_SESSION_CONTROL_RUN_ID, 
	checkout_type AS CHECKOUT_TYPE_CODE, 
	check_out_message AS CHECKOUT_MESSAGE, 
	TARGET_NAME, 
	TARGET_COUNT, 
	SOURCE_DT, 
	User_ID AS CREATED_USER_ID, 
	CREATED_DATE, 
	User_ID AS MODIFIED_USER_ID, 
	MODIFIED_DATE
	FROM UPD_CHECKOUT_TABLE_INS_counts
),