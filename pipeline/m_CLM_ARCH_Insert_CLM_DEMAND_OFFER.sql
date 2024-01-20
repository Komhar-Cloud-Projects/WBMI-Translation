WITH
SQ_clm_demand_offer_stage AS (
	SELECT
		clm_demand_offer_stage_id,
		tch_claim_nbr,
		tch_client_id,
		create_ts,
		demand_offer_dt,
		demand_amt,
		offer_amt,
		create_user_id,
		damage_desc,
		extract_date,
		as_of_date,
		record_count,
		source_system_id
	FROM clm_demand_offer_stage
),
EXPTRANS AS (
	SELECT
	clm_demand_offer_stage_id,
	tch_claim_nbr,
	tch_client_id,
	create_ts,
	demand_offer_dt,
	demand_amt,
	offer_amt,
	create_user_id,
	damage_desc,
	extract_date,
	as_of_date,
	record_count,
	source_system_id,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AUDIT_ID
	FROM SQ_clm_demand_offer_stage
),
arch_clm_demand_offer_stage AS (
	INSERT INTO arch_clm_demand_offer_stage
	(clm_demand_offer_stage_id, tch_claim_nbr, tch_client_id, create_ts, demand_offer_dt, demand_amt, offer_amt, create_user_id, damage_desc, extract_date, as_of_date, record_count, source_system_id, audit_id)
	SELECT 
	CLM_DEMAND_OFFER_STAGE_ID, 
	TCH_CLAIM_NBR, 
	TCH_CLIENT_ID, 
	CREATE_TS, 
	DEMAND_OFFER_DT, 
	DEMAND_AMT, 
	OFFER_AMT, 
	CREATE_USER_ID, 
	DAMAGE_DESC, 
	EXTRACT_DATE, 
	AS_OF_DATE, 
	RECORD_COUNT, 
	SOURCE_SYSTEM_ID, 
	AUDIT_ID AS AUDIT_ID
	FROM EXPTRANS
),