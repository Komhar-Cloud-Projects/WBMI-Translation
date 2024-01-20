WITH
SQ_clt_ref_relation_stage AS (
	SELECT clt_ref_relation_stage.clt_ref_relation_stage_id, clt_ref_relation_stage.client_id, clt_ref_relation_stage.cirf_ref_seq_nbr, clt_ref_relation_stage.history_vld_nbr, clt_ref_relation_stage.cirf_eff_dt, clt_ref_relation_stage.cirf_ref_id, clt_ref_relation_stage.ref_typ_cd, clt_ref_relation_stage.cirf_exp_dt, clt_ref_relation_stage.user_id, clt_ref_relation_stage.status_cd, clt_ref_relation_stage.terminal_id, clt_ref_relation_stage.cirf_eff_acy_ts, clt_ref_relation_stage.cirf_exp_acy_ts, clt_ref_relation_stage.extract_date, clt_ref_relation_stage.as_of_date, clt_ref_relation_stage.record_count, clt_ref_relation_stage.source_system_id 
	FROM
	 clt_ref_relation_stage
	WHERE clt_ref_relation_stage.cirf_eff_acy_ts >= '@{pipeline().parameters.SELECTION_START_TS}'
),
EXP_clt_ref_relation AS (
	SELECT
	clt_ref_relation_stage_id,
	client_id,
	cirf_ref_seq_nbr,
	history_vld_nbr,
	cirf_eff_dt,
	cirf_ref_id,
	ref_typ_cd,
	cirf_exp_dt,
	user_id,
	status_cd,
	terminal_id,
	cirf_eff_acy_ts,
	cirf_exp_acy_ts,
	extract_date,
	as_of_date,
	record_count,
	source_system_id,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AUDIT_ID_OP
	FROM SQ_clt_ref_relation_stage
),
arch_clt_ref_relation_stage AS (
	INSERT INTO arch_clt_ref_relation_stage
	(clt_ref_relation_stage_id, client_id, cirf_ref_seq_nbr, history_vld_nbr, cirf_eff_dt, cirf_ref_id, ref_typ_cd, cirf_exp_dt, user_id, status_cd, terminal_id, cirf_eff_acy_ts, cirf_exp_acy_ts, extract_date, as_of_date, record_count, source_system_id, audit_id)
	SELECT 
	CLT_REF_RELATION_STAGE_ID, 
	CLIENT_ID, 
	CIRF_REF_SEQ_NBR, 
	HISTORY_VLD_NBR, 
	CIRF_EFF_DT, 
	CIRF_REF_ID, 
	REF_TYP_CD, 
	CIRF_EXP_DT, 
	USER_ID, 
	STATUS_CD, 
	TERMINAL_ID, 
	CIRF_EFF_ACY_TS, 
	CIRF_EXP_ACY_TS, 
	EXTRACT_DATE, 
	AS_OF_DATE, 
	RECORD_COUNT, 
	SOURCE_SYSTEM_ID, 
	AUDIT_ID_OP AS AUDIT_ID
	FROM EXP_clt_ref_relation
),