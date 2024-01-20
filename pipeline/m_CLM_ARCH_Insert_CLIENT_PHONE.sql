WITH
SQ_source AS (
	SELECT client_phone_stage.client_phone_stage_id, client_phone_stage.client_id, client_phone_stage.ciph_phn_seq_nbr, client_phone_stage.history_vld_nbr, client_phone_stage.ciph_eff_dt, client_phone_stage.ciph_phn_nbr, client_phone_stage.phn_typ_cd, client_phone_stage.ciph_xrf_id, client_phone_stage.user_id, client_phone_stage.status_cd, client_phone_stage.terminal_id, client_phone_stage.ciph_exp_dt, client_phone_stage.ciph_eff_acy_ts, client_phone_stage.ciph_exp_acy_ts, client_phone_stage.extract_date, client_phone_stage.as_of_date, client_phone_stage.record_count, client_phone_stage.source_system_id 
	FROM
	 client_phone_stage
	WHERE 
	client_phone_stage.CIPH_EFF_ACY_TS >= '@{pipeline().parameters.SELECTION_START_TS}'
),
EXP_values AS (
	SELECT
	client_phone_stage_id,
	client_id,
	ciph_phn_seq_nbr,
	history_vld_nbr,
	ciph_eff_dt,
	ciph_phn_nbr,
	phn_typ_cd,
	ciph_xrf_id,
	user_id,
	status_cd,
	terminal_id,
	ciph_exp_dt,
	ciph_eff_acy_ts,
	ciph_exp_acy_ts,
	extract_date,
	as_of_date,
	record_count,
	source_system_id,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AUDIT_ID
	FROM SQ_source
),
arch_client_phone_stage AS (
	INSERT INTO arch_client_phone_stage
	(client_phone_stage_id, client_id, ciph_phn_seq_nbr, history_vld_nbr, ciph_eff_dt, ciph_phn_nbr, phn_typ_cd, ciph_xrf_id, user_id, status_cd, terminal_id, ciph_exp_dt, ciph_eff_acy_ts, ciph_exp_acy_ts)
	SELECT 
	CLIENT_PHONE_STAGE_ID, 
	CLIENT_ID, 
	CIPH_PHN_SEQ_NBR, 
	HISTORY_VLD_NBR, 
	CIPH_EFF_DT, 
	CIPH_PHN_NBR, 
	PHN_TYP_CD, 
	CIPH_XRF_ID, 
	USER_ID, 
	STATUS_CD, 
	TERMINAL_ID, 
	CIPH_EXP_DT, 
	CIPH_EFF_ACY_TS, 
	CIPH_EXP_ACY_TS
	FROM EXP_values
),