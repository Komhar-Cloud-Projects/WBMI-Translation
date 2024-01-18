WITH
SQ_clm_occurrence_nbr_stage AS (
	SELECT clm_occurrence_nbr_stage.clm_occurrence_nbr_stage_id, clm_occurrence_nbr_stage.con_claim_nbr, clm_occurrence_nbr_stage.con_policy_id, clm_occurrence_nbr_stage.con_occurrence_nbr, clm_occurrence_nbr_stage.con_entry_opr_id, clm_occurrence_nbr_stage.con_create_ts, clm_occurrence_nbr_stage.con_loss_dt, clm_occurrence_nbr_stage.extract_date, clm_occurrence_nbr_stage.as_of_date, clm_occurrence_nbr_stage.record_count, clm_occurrence_nbr_stage.source_system_id 
	FROM
	 clm_occurrence_nbr_stage
	WHERE
	clm_occurrence_nbr_stage.con_create_ts >= '@{pipeline().parameters.SELECTION_START_TS}'
),
EXP_clm_occurrence_nbr AS (
	SELECT
	clm_occurrence_nbr_stage_id,
	con_claim_nbr,
	con_policy_id,
	con_occurrence_nbr,
	con_entry_opr_id,
	con_create_ts,
	con_loss_dt,
	extract_date,
	as_of_date,
	record_count,
	source_system_id,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AUDIT_ID_OP
	FROM SQ_clm_occurrence_nbr_stage
),
arch_clm_occurrence_nbr_stage AS (
	INSERT INTO arch_clm_occurrence_nbr_stage
	(clm_occurrence_nbr_stage_id, con_claim_nbr, con_policy_id, con_occurrence_nbr, con_entry_opr_id, con_create_ts, con_loss_dt, extract_date, as_of_date, record_count, source_system_id, audit_id)
	SELECT 
	CLM_OCCURRENCE_NBR_STAGE_ID, 
	CON_CLAIM_NBR, 
	CON_POLICY_ID, 
	CON_OCCURRENCE_NBR, 
	CON_ENTRY_OPR_ID, 
	CON_CREATE_TS, 
	CON_LOSS_DT, 
	EXTRACT_DATE, 
	AS_OF_DATE, 
	RECORD_COUNT, 
	SOURCE_SYSTEM_ID, 
	AUDIT_ID_OP AS AUDIT_ID
	FROM EXP_clm_occurrence_nbr
),