WITH
SQ_clm_lien_details_stage AS (
	SELECT
		clm_lien_details_stage_id,
		tch_claim_nbr,
		tch_client_id,
		lien_client_id,
		lien_role,
		lien_amt,
		create_ts,
		create_user_id,
		update_ts,
		update_user_id,
		extract_date,
		as_of_date,
		record_count,
		source_system_id
	FROM clm_lien_details_stage
),
EXP_CLM_LIEN_DETAILS AS (
	SELECT
	clm_lien_details_stage_id,
	tch_claim_nbr,
	tch_client_id,
	lien_client_id,
	lien_role,
	lien_amt,
	create_ts,
	create_user_id,
	update_ts,
	update_user_id,
	extract_date,
	as_of_date,
	record_count,
	source_system_id,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AUDIT_ID_OP
	FROM SQ_clm_lien_details_stage
),
Shortcut_to_arch_clm_lien_details_stage AS (
	INSERT INTO arch_clm_lien_details_stage
	(clm_lien_details_stage_id, tch_claim_nbr, tch_client_id, lien_client_id, lien_role, lien_amt, create_ts, create_user_id, update_ts, update_user_id, extract_date, as_of_date, record_count, source_system_id, audit_id)
	SELECT 
	CLM_LIEN_DETAILS_STAGE_ID, 
	TCH_CLAIM_NBR, 
	TCH_CLIENT_ID, 
	LIEN_CLIENT_ID, 
	LIEN_ROLE, 
	LIEN_AMT, 
	CREATE_TS, 
	CREATE_USER_ID, 
	UPDATE_TS, 
	UPDATE_USER_ID, 
	EXTRACT_DATE, 
	AS_OF_DATE, 
	RECORD_COUNT, 
	SOURCE_SYSTEM_ID, 
	AUDIT_ID_OP AS AUDIT_ID
	FROM EXP_CLM_LIEN_DETAILS
),