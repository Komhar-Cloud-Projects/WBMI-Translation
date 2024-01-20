WITH
SQ_CLM_CLMT_DAMAGES_STAGE AS (
	SELECT
		clm_clmt_damage_stage_id,
		tch_claim_nbr,
		tch_client_id,
		damage_seq,
		damage_cd,
		damage_amt,
		damage_desc,
		create_ts,
		create_user_id,
		update_ts,
		update_user_id,
		damage_high_amt,
		damage_type,
		extract_date,
		as_of_date,
		record_count,
		source_system_id
	FROM CLM_CLMT_DAMAGES_STAGE
),
EXP_CLM_CLMT_DAMAGES_STAGE AS (
	SELECT
	clm_clmt_damage_stage_id,
	tch_claim_nbr,
	tch_client_id,
	damage_seq,
	damage_cd,
	damage_amt,
	damage_desc,
	create_ts,
	create_user_id,
	update_ts,
	update_user_id,
	damage_high_amt,
	damage_type,
	extract_date,
	as_of_date,
	record_count,
	source_system_id,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id_op
	FROM SQ_CLM_CLMT_DAMAGES_STAGE
),
ARCH_CLM_CLMT_DAMAGES_STAGE AS (
	INSERT INTO arch_clm_clmt_damages_stage
	(clm_clmt_damage_stage_id, tch_claim_nbr, tch_client_id, damage_seq, damage_cd, damage_amt, damage_desc, create_ts, create_user_id, update_ts, update_user_id, damage_high_amt, damage_type, extract_date, as_of_date, record_count, source_system_id, audit_id)
	SELECT 
	CLM_CLMT_DAMAGE_STAGE_ID, 
	TCH_CLAIM_NBR, 
	TCH_CLIENT_ID, 
	DAMAGE_SEQ, 
	DAMAGE_CD, 
	DAMAGE_AMT, 
	DAMAGE_DESC, 
	CREATE_TS, 
	CREATE_USER_ID, 
	UPDATE_TS, 
	UPDATE_USER_ID, 
	DAMAGE_HIGH_AMT, 
	DAMAGE_TYPE, 
	EXTRACT_DATE, 
	AS_OF_DATE, 
	RECORD_COUNT, 
	SOURCE_SYSTEM_ID, 
	audit_id_op AS AUDIT_ID
	FROM EXP_CLM_CLMT_DAMAGES_STAGE
),