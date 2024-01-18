WITH
SQ_client_tax_stage AS (
	SELECT client_tax_stage.client_tax_stage_id, client_tax_stage.client_id, client_tax_stage.citx_tax_seq_nbr, client_tax_stage.history_vld_nbr, client_tax_stage.effective_dt, client_tax_stage.citx_tax_id, client_tax_stage.tax_type_cd, client_tax_stage.citx_tax_st_cd, client_tax_stage.citx_tax_ctr_cd, client_tax_stage.user_id, client_tax_stage.status_cd, client_tax_stage.terminal_id, client_tax_stage.expiration_dt, client_tax_stage.effective_acy_ts, client_tax_stage.expiration_acy_ts, client_tax_stage.extract_date, client_tax_stage.as_of_date, client_tax_stage.record_count, client_tax_stage.source_system_id 
	FROM
	 client_tax_stage
	WHERE
	client_tax_stage.effective_acy_ts >= '@{pipeline().parameters.SELECTION_START_TS}'
),
EXP_CLIENT_TAX_STAGE AS (
	SELECT
	client_tax_stage_id,
	client_id,
	citx_tax_seq_nbr,
	history_vld_nbr,
	effective_dt,
	citx_tax_id,
	tax_type_cd,
	citx_tax_st_cd,
	citx_tax_ctr_cd,
	user_id,
	status_cd,
	terminal_id,
	expiration_dt,
	effective_acy_ts,
	expiration_acy_ts,
	extract_date,
	as_of_date,
	record_count,
	source_system_id,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AUDIT_ID_OP
	FROM SQ_client_tax_stage
),
arch_client_tax_stage AS (
	INSERT INTO arch_client_tax_stage
	(client_tax_stage_id, client_id, citx_tax_seq_nbr, history_vld_nbr, effective_dt, citx_tax_id, tax_type_cd, citx_tax_st_cd, citx_tax_ctr_cd, user_id, status_cd, terminal_id, expiration_dt, effective_acy_ts, expiration_acy_ts, extract_date, as_of_date, record_count, source_system_id, audit_id)
	SELECT 
	CLIENT_TAX_STAGE_ID, 
	CLIENT_ID, 
	CITX_TAX_SEQ_NBR, 
	HISTORY_VLD_NBR, 
	EFFECTIVE_DT, 
	CITX_TAX_ID, 
	TAX_TYPE_CD, 
	CITX_TAX_ST_CD, 
	CITX_TAX_CTR_CD, 
	USER_ID, 
	STATUS_CD, 
	TERMINAL_ID, 
	EXPIRATION_DT, 
	EFFECTIVE_ACY_TS, 
	EXPIRATION_ACY_TS, 
	EXTRACT_DATE, 
	AS_OF_DATE, 
	RECORD_COUNT, 
	SOURCE_SYSTEM_ID, 
	AUDIT_ID_OP AS AUDIT_ID
	FROM EXP_CLIENT_TAX_STAGE
),