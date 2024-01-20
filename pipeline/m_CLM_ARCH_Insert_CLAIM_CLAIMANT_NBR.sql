WITH
SQ_claim_claimant_nbr_stage AS (
	SELECT claim_claimant_nbr_stage.claim_claimant_nbr_id, claim_claimant_nbr_stage.ccn_claim_nbr, claim_claimant_nbr_stage.ccn_client_id, claim_claimant_nbr_stage.ccn_object_type_cd, claim_claimant_nbr_stage.ccn_object_seq_nbr, claim_claimant_nbr_stage.ccn_cov_type_cd, claim_claimant_nbr_stage.ccn_cov_seq_nbr, claim_claimant_nbr_stage.ccn_bur_cau_los_cd, claim_claimant_nbr_stage.ccn_claimant_nbr, claim_claimant_nbr_stage.ccn_entry_opr_id, claim_claimant_nbr_stage.ccn_create_ts, claim_claimant_nbr_stage.extract_date, claim_claimant_nbr_stage.as_of_date, claim_claimant_nbr_stage.record_count, claim_claimant_nbr_stage.source_system_id 
	FROM
	 claim_claimant_nbr_stage
	WHERE
	claim_claimant_nbr_stage.ccn_create_ts >= '@{pipeline().parameters.SELECTION_START_TS}'
),
EXP_DEFAULT AS (
	SELECT
	claim_claimant_nbr_id,
	ccn_claim_nbr,
	ccn_client_id,
	ccn_object_type_cd,
	ccn_object_seq_nbr,
	ccn_cov_type_cd,
	ccn_cov_seq_nbr,
	ccn_bur_cau_los_cd,
	ccn_claimant_nbr,
	ccn_entry_opr_id,
	ccn_create_ts,
	extract_date,
	as_of_date,
	record_count,
	source_system_id,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id
	FROM SQ_claim_claimant_nbr_stage
),
arch_claim_claimant_nbr_stage AS (
	INSERT INTO arch_claim_claimant_nbr_stage
	(claim_claimant_nbr_id, ccn_claim_nbr, ccn_client_id, ccn_object_type_cd, ccn_object_seq_nbr, ccn_cov_type_cd, ccn_cov_seq_nbr, ccn_bur_cau_los_cd, ccn_claimant_nbr, ccn_entry_opr_id, ccn_create_ts, extract_date, as_of_date, record_count, source_system_id, audit_id)
	SELECT 
	CLAIM_CLAIMANT_NBR_ID, 
	CCN_CLAIM_NBR, 
	CCN_CLIENT_ID, 
	CCN_OBJECT_TYPE_CD, 
	CCN_OBJECT_SEQ_NBR, 
	CCN_COV_TYPE_CD, 
	CCN_COV_SEQ_NBR, 
	CCN_BUR_CAU_LOS_CD, 
	CCN_CLAIMANT_NBR, 
	CCN_ENTRY_OPR_ID, 
	CCN_CREATE_TS, 
	EXTRACT_DATE, 
	AS_OF_DATE, 
	RECORD_COUNT, 
	SOURCE_SYSTEM_ID, 
	AUDIT_ID
	FROM EXP_DEFAULT
),