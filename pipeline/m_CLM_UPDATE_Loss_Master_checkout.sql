WITH
SQ_Loss_Master_Fact_and_Calendar_Dim AS (
	select top 1
	C.clndr_date, 
	LMF.audit_id, 
	LMF.loss_master_run_date_id  
	from 
	loss_master_fact LMF 
	  inner join calendar_dim C  on C.clndr_id=LMF.loss_master_run_date_id
	where audit_id=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
),
EXP_input AS (
	SELECT
	clndr_date,
	audit_id,
	loss_master_run_date_id,
	@{pipeline().parameters.SUBJECTAREA} AS subject_area
	FROM SQ_Loss_Master_Fact_and_Calendar_Dim
),
SQL_wbmi_checkout AS (-- SQL_wbmi_checkout

	##############################################

	# TODO: Place holder for Custom transformation

	##############################################
),
EXP_sql_output AS (
	SELECT
	wbmi_checkout_id,
	clndr_date_output
	FROM SQL_wbmi_checkout
),
FIL_nulls AS (
	SELECT
	wbmi_checkout_id, 
	clndr_date_output
	FROM EXP_sql_output
	WHERE NOT ISNULL(wbmi_checkout_id)
),
UPD_UPDATE AS (
	SELECT
	wbmi_checkout_id, 
	clndr_date_output
	FROM FIL_nulls
),
wbmi_checkout AS (
	MERGE INTO wbmi_checkout AS T
	USING UPD_UPDATE AS S
	ON T.wbmi_checkout_id = S.wbmi_checkout_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.source_dt = S.clndr_date_output, T.target_dt = S.clndr_date_output
),