WITH
SQ_clt_clt_obj_rel_stage AS (
	SELECT clt_clt_obj_rel_stage.clt_clt_obj_rel_stage_id,
	       clt_clt_obj_rel_stage.tch_object_key          ,
	       clt_clt_obj_rel_stage.client_id               ,
	       clt_clt_obj_rel_stage.history_vld_nbr         ,
	       clt_clt_obj_rel_stage.ciro_eff_dt             ,
	       clt_clt_obj_rel_stage.ciro_obj_seq_nbr        ,
	       clt_clt_obj_rel_stage.rlt_typ_cd              ,
	       clt_clt_obj_rel_stage.ciro_xrf_id             ,
	       clt_clt_obj_rel_stage.xrf_typ_cd              ,
	       clt_clt_obj_rel_stage.user_id                 ,
	       clt_clt_obj_rel_stage.status_cd               ,
	       clt_clt_obj_rel_stage.terminal_id             ,
	       clt_clt_obj_rel_stage.ciro_exp_dt             ,
	  	clt_clt_obj_rel_stage.ciro_eff_acy_ts      ,
	       clt_clt_obj_rel_stage.ciro_exp_acy_ts         ,
	       clt_clt_obj_rel_stage.extract_date            ,
	       clt_clt_obj_rel_stage.as_of_date              ,
	       clt_clt_obj_rel_stage.record_count            ,
	       clt_clt_obj_rel_stage.source_system_id
	FROM   clt_clt_obj_rel_stage
	WHERE clt_clt_obj_rel_stage.ciro_eff_acy_ts >='@{pipeline().parameters.SELECTION_START_TS}'
),
EXP_clt_clt_obj_rel AS (
	SELECT
	clt_clt_obj_rel_stage_id,
	tch_object_key,
	client_id,
	history_vld_nbr,
	ciro_eff_dt,
	ciro_obj_seq_nbr,
	rlt_typ_cd,
	ciro_xrf_id,
	xrf_typ_cd,
	user_id,
	status_cd,
	terminal_id,
	ciro_exp_dt,
	ciro_eff_acy_ts,
	ciro_exp_acy_ts,
	extract_date,
	as_of_date,
	record_count,
	source_system_id,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AUDIT_ID_OP
	FROM SQ_clt_clt_obj_rel_stage
),
arch_clt_clt_obj_rel_stage AS (
	INSERT INTO arch_clt_clt_obj_rel_stage
	(clt_clt_obj_rel_stage_id, tch_object_key, client_id, history_vld_nbr, ciro_eff_dt, ciro_obj_seq_nbr, rlt_typ_cd, ciro_xrf_id, xrf_typ_cd, user_id, status_cd, terminal_id, ciro_exp_dt, ciro_eff_acy_ts, ciro_exp_acy_ts, extract_date, as_of_date, record_count, source_system_id, audit_id)
	SELECT 
	CLT_CLT_OBJ_REL_STAGE_ID, 
	TCH_OBJECT_KEY, 
	CLIENT_ID, 
	HISTORY_VLD_NBR, 
	CIRO_EFF_DT, 
	CIRO_OBJ_SEQ_NBR, 
	RLT_TYP_CD, 
	CIRO_XRF_ID, 
	XRF_TYP_CD, 
	USER_ID, 
	STATUS_CD, 
	TERMINAL_ID, 
	CIRO_EXP_DT, 
	CIRO_EFF_ACY_TS, 
	CIRO_EXP_ACY_TS, 
	EXTRACT_DATE, 
	AS_OF_DATE, 
	RECORD_COUNT, 
	SOURCE_SYSTEM_ID, 
	AUDIT_ID_OP AS AUDIT_ID
	FROM EXP_clt_clt_obj_rel
),