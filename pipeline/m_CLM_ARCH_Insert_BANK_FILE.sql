WITH
SQ_bank_file_stage AS (
	SELECT
		bank_file_stage_id,
		ws_type,
		ws_bank_nbr,
		ws_acct_nbr,
		ws_trans_type,
		ws_check_nbr,
		ws_amount,
		ws_trans_code,
		ws_trans_date,
		ws_control_nbr,
		ws_trans_status,
		ws_total_cr_amount,
		ws_total_cr_count,
		ws_total_db_amount,
		ws_total_db_count,
		ws_processing_date,
		ws_cycle_to_date_amount,
		ws_balance_sign,
		extract_date,
		as_of_date,
		record_count,
		source_system_id
	FROM bank_file_stage
),
EXP_bank_file AS (
	SELECT
	bank_file_stage_id,
	ws_type,
	ws_bank_nbr,
	ws_acct_nbr,
	ws_trans_type,
	ws_check_nbr,
	ws_amount,
	ws_trans_code,
	ws_trans_date,
	ws_control_nbr,
	ws_trans_status,
	ws_total_cr_amount,
	ws_total_cr_count,
	ws_total_db_amount,
	ws_total_db_count,
	ws_processing_date,
	ws_cycle_to_date_amount,
	ws_balance_sign,
	extract_date,
	as_of_date,
	record_count,
	source_system_id,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AUDIT_ID_OP
	FROM SQ_bank_file_stage
),
arch_bank_file_stage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.arch_bank_file_stage
	(bank_file_stage_id, ws_type, ws_bank_nbr, ws_acct_nbr, ws_trans_type, ws_check_nbr, ws_amount, ws_trans_code, ws_trans_date, ws_control_nbr, ws_trans_status, ws_total_cr_amount, ws_total_cr_count, ws_total_db_amount, ws_total_db_count, ws_processing_date, ws_cycle_to_date_amount, ws_balance_sign, extract_date, as_of_date, record_count, source_system_id, audit_id)
	SELECT 
	BANK_FILE_STAGE_ID, 
	WS_TYPE, 
	WS_BANK_NBR, 
	WS_ACCT_NBR, 
	WS_TRANS_TYPE, 
	WS_CHECK_NBR, 
	WS_AMOUNT, 
	WS_TRANS_CODE, 
	WS_TRANS_DATE, 
	WS_CONTROL_NBR, 
	WS_TRANS_STATUS, 
	WS_TOTAL_CR_AMOUNT, 
	WS_TOTAL_CR_COUNT, 
	WS_TOTAL_DB_AMOUNT, 
	WS_TOTAL_DB_COUNT, 
	WS_PROCESSING_DATE, 
	WS_CYCLE_TO_DATE_AMOUNT, 
	WS_BALANCE_SIGN, 
	EXTRACT_DATE, 
	AS_OF_DATE, 
	RECORD_COUNT, 
	SOURCE_SYSTEM_ID, 
	AUDIT_ID_OP AS AUDIT_ID
	FROM EXP_bank_file
),