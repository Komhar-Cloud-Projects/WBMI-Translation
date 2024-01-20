WITH
SQ_clm_tot_loss_hist_stage AS (
	SELECT
		clm_tot_loss_hist_stage_id,
		tch_claim_nbr,
		tch_client_id,
		object_type_cd,
		object_seq_nbr,
		cov_type_cd,
		cov_seq_nbr,
		bur_cause_loss,
		seq_nbr,
		add_uuid,
		vehicle_vin,
		add_action,
		add_action_ts,
		loss_date,
		loss_owner,
		new_owner,
		create_ts,
		create_user_id,
		update_ts,
		update_user_id,
		extract_date,
		as_of_date,
		record_count,
		source_system_id
	FROM clm_tot_loss_hist_stage1
),
EXP_get_values AS (
	SELECT
	clm_tot_loss_hist_stage_id,
	tch_claim_nbr,
	tch_client_id,
	object_type_cd,
	object_seq_nbr,
	cov_type_cd,
	cov_seq_nbr,
	bur_cause_loss,
	seq_nbr,
	add_uuid,
	vehicle_vin,
	add_action,
	add_action_ts,
	loss_date,
	loss_owner,
	new_owner,
	create_ts,
	create_user_id,
	update_ts,
	update_user_id,
	extract_date,
	as_of_date,
	record_count,
	source_system_id,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id
	FROM SQ_clm_tot_loss_hist_stage
),
arch_clm_tot_loss_hist_stage AS (
	INSERT INTO arch_clm_tot_loss_hist_stage
	(clm_tot_loss_hist_stage_id, tch_claim_nbr, tch_client_id, object_type_cd, object_seq_nbr, cov_type_cd, cov_seq_nbr, bur_cause_loss, seq_nbr, add_uuid, vehicle_vin, add_action, add_action_ts, loss_date, loss_owner, new_owner, create_ts, create_user_id, update_ts, update_user_id, extract_date, as_of_date, record_count, source_system_id, audit_id)
	SELECT 
	CLM_TOT_LOSS_HIST_STAGE_ID, 
	TCH_CLAIM_NBR, 
	TCH_CLIENT_ID, 
	OBJECT_TYPE_CD, 
	OBJECT_SEQ_NBR, 
	COV_TYPE_CD, 
	COV_SEQ_NBR, 
	BUR_CAUSE_LOSS, 
	SEQ_NBR, 
	ADD_UUID, 
	VEHICLE_VIN, 
	ADD_ACTION, 
	ADD_ACTION_TS, 
	LOSS_DATE, 
	LOSS_OWNER, 
	NEW_OWNER, 
	CREATE_TS, 
	CREATE_USER_ID, 
	UPDATE_TS, 
	UPDATE_USER_ID, 
	EXTRACT_DATE, 
	AS_OF_DATE, 
	RECORD_COUNT, 
	SOURCE_SYSTEM_ID, 
	AUDIT_ID
	FROM EXP_get_values
),