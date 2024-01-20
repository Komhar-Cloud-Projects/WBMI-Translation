WITH
SQ_CLM_CLT_EOR_STAGE AS (
	SELECT clm_clt_eor_stage.clm_clt_eor_id, clm_clt_eor_stage.cce_tch_bill_nbr, clm_clt_eor_stage.cce_claim_nbr, clm_clt_eor_stage.cce_provider_cd, clm_clt_eor_stage.cce_create_ts, clm_clt_eor_stage.cce_paid_ts, clm_clt_eor_stage.cce_paid_user_id, clm_clt_eor_stage.cce_client_id, clm_clt_eor_stage.cce_client_fst_nm, clm_clt_eor_stage.cce_client_lst_nm, clm_clt_eor_stage.cce_eor_status, clm_clt_eor_stage.extract_date, clm_clt_eor_stage.as_of_date, clm_clt_eor_stage.record_count, clm_clt_eor_stage.source_system_id, clm_clt_eor_stage.denial_reason_cd, clm_clt_eor_stage.cce_draft_nbr, clm_clt_eor_stage.modified_ts 
	FROM
	clm_clt_eor_stage
	WHERE
	clm_clt_eor_stage.cce_create_ts >= '@{pipeline().parameters.SELECTION_START_TS}'
	OR
	clm_clt_eor_stage.cce_paid_ts >= '@{pipeline().parameters.SELECTION_START_TS}'
	OR
	clm_clt_eor_stage.modified_ts >= '@{pipeline().parameters.SELECTION_START_TS}'
),
EXPTRANS AS (
	SELECT
	CLM_CLT_EOR_ID,
	CCE_TCH_BILL_NBR,
	CCE_CLAIM_NBR,
	CCE_PROVIDER_CD,
	CCE_CREATE_TS,
	CCE_PAID_TS,
	CCE_PAID_USER_ID,
	CCE_CLIENT_ID,
	CCE_CLIENT_FST_NM,
	CCE_CLIENT_LST_NM,
	CCE_EOR_STATUS,
	EXTRACT_DATE,
	AS_OF_DATE,
	RECORD_COUNT,
	SOURCE_SYSTEM_ID,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AUDIT_ID_OP,
	denial_reason_cd,
	cce_draft_nbr,
	modified_ts
	FROM SQ_CLM_CLT_EOR_STAGE
),
ARCH_CLM_CLT_EOR_STAGE AS (
	INSERT INTO ARCH_CLM_CLT_EOR_STAGE
	(clm_clt_eor_id, cce_tch_bill_nbr, cce_claim_nbr, cce_provider_cd, cce_create_ts, cce_paid_ts, cce_paid_user_id, cce_client_id, cce_client_fst_nm, cce_client_lst_nm, cce_eor_status, extract_date, as_of_date, record_count, source_system_id, audit_id, denial_reason_cd, cce_draft_nbr, modified_ts)
	SELECT 
	CLM_CLT_EOR_ID AS CLM_CLT_EOR_ID, 
	CCE_TCH_BILL_NBR AS CCE_TCH_BILL_NBR, 
	CCE_CLAIM_NBR AS CCE_CLAIM_NBR, 
	CCE_PROVIDER_CD AS CCE_PROVIDER_CD, 
	CCE_CREATE_TS AS CCE_CREATE_TS, 
	CCE_PAID_TS AS CCE_PAID_TS, 
	CCE_PAID_USER_ID AS CCE_PAID_USER_ID, 
	CCE_CLIENT_ID AS CCE_CLIENT_ID, 
	CCE_CLIENT_FST_NM AS CCE_CLIENT_FST_NM, 
	CCE_CLIENT_LST_NM AS CCE_CLIENT_LST_NM, 
	CCE_EOR_STATUS AS CCE_EOR_STATUS, 
	EXTRACT_DATE AS EXTRACT_DATE, 
	AS_OF_DATE AS AS_OF_DATE, 
	RECORD_COUNT AS RECORD_COUNT, 
	SOURCE_SYSTEM_ID AS SOURCE_SYSTEM_ID, 
	AUDIT_ID_OP AS AUDIT_ID, 
	DENIAL_REASON_CD, 
	CCE_DRAFT_NBR, 
	MODIFIED_TS
	FROM EXPTRANS
),