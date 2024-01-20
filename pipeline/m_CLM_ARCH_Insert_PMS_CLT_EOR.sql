WITH
SQ_pms_clt_eor_stage AS (
	SELECT pms_clt_eor_stage.pms_clt_eor_stage_id, pms_clt_eor_stage.pce_tch_bill_nbr, pms_clt_eor_stage.pce_policy_sym, pms_clt_eor_stage.pce_policy_num, pms_clt_eor_stage.pce_policy_mod, pms_clt_eor_stage.pce_date_of_loss, pms_clt_eor_stage.pce_occurrence, pms_clt_eor_stage.pce_provider_cd, pms_clt_eor_stage.pce_create_ts, pms_clt_eor_stage.pce_paid_ts, pms_clt_eor_stage.pce_paid_user_id, pms_clt_eor_stage.pce_client_id, pms_clt_eor_stage.pce_client_fst_nm, pms_clt_eor_stage.pce_client_lst_nm, pms_clt_eor_stage.pce_eor_status, pms_clt_eor_stage.modified_ts, pms_clt_eor_stage.check_number, pms_clt_eor_stage.amount_paid_by_chk, pms_clt_eor_stage.denial_reason_cd, pms_clt_eor_stage.extract_date, pms_clt_eor_stage.as_of_date, pms_clt_eor_stage.record_count, pms_clt_eor_stage.source_system_id 
	FROM
	 pms_clt_eor_stage
	WHERE
	pms_clt_eor_stage.pce_create_ts >= '@{pipeline().parameters.SELECTION_START_TS}'
	OR
	pms_clt_eor_stage.pce_paid_ts >= '@{pipeline().parameters.SELECTION_START_TS}'
	OR
	pms_clt_eor_stage.modified_ts >= '@{pipeline().parameters.SELECTION_START_TS}'
),
EXPTRANS AS (
	SELECT
	pms_clt_eor_stage_id,
	pce_tch_bill_nbr,
	pce_policy_sym,
	pce_policy_num,
	pce_policy_mod,
	pce_date_of_loss,
	pce_occurrence,
	pce_provider_cd,
	pce_create_ts,
	pce_paid_ts,
	pce_paid_user_id,
	pce_client_id,
	pce_client_fst_nm,
	pce_client_lst_nm,
	pce_eor_status,
	modified_ts,
	check_number,
	amount_paid_by_chk,
	denial_reason_cd,
	extract_date,
	as_of_date,
	record_count,
	source_system_id,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AUDIT_ID_OP
	FROM SQ_pms_clt_eor_stage
),
arch_pms_clt_eor_stage AS (
	INSERT INTO arch_pms_clt_eor_stage
	(pms_clt_eor_stage_id, pce_tch_bill_nbr, pce_policy_sym, pce_policy_num, pce_policy_mod, pce_date_of_loss, pce_occurrence, pce_provider_cd, pce_create_ts, pce_paid_ts, pce_paid_user_id, pce_client_id, pce_client_fst_nm, pce_client_lst_nm, pce_eor_status, modified_ts, check_number, amount_paid_by_chk, denial_reason_cd, extract_date, as_of_date, record_count, source_system_id, audit_id)
	SELECT 
	PMS_CLT_EOR_STAGE_ID, 
	PCE_TCH_BILL_NBR, 
	PCE_POLICY_SYM, 
	PCE_POLICY_NUM, 
	PCE_POLICY_MOD, 
	PCE_DATE_OF_LOSS, 
	PCE_OCCURRENCE, 
	PCE_PROVIDER_CD, 
	PCE_CREATE_TS, 
	PCE_PAID_TS, 
	PCE_PAID_USER_ID, 
	PCE_CLIENT_ID, 
	PCE_CLIENT_FST_NM, 
	PCE_CLIENT_LST_NM, 
	PCE_EOR_STATUS, 
	MODIFIED_TS, 
	CHECK_NUMBER, 
	AMOUNT_PAID_BY_CHK, 
	DENIAL_REASON_CD, 
	EXTRACT_DATE, 
	AS_OF_DATE, 
	RECORD_COUNT, 
	SOURCE_SYSTEM_ID, 
	AUDIT_ID_OP AS AUDIT_ID
	FROM EXPTRANS
),