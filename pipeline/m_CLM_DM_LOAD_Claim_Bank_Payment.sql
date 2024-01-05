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
EXP_Source AS (
	SELECT
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS O_audit_id,
	sysdate AS O_eff_from_date,
	-- *INF*: to_date('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	to_date('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS O_eff_to_date,
	sysdate AS O_created_date,
	sysdate AS O_modified_date,
	ws_type,
	ws_bank_nbr,
	ws_acct_nbr,
	ws_trans_type,
	ws_check_nbr,
	ws_amount,
	ws_trans_code,
	ws_trans_date,
	ws_trans_status,
	ws_total_db_amount,
	ws_total_db_count,
	-- *INF*: IIF(ws_type = 'D' AND ws_trans_type = 'K', ROUND(v_total_db_amount_chk + ws_amount,2), v_total_db_amount_chk)
	IFF(ws_type = 'D' AND ws_trans_type = 'K', ROUND(v_total_db_amount_chk + ws_amount, 2), v_total_db_amount_chk) AS v_total_db_amount_chk,
	-- *INF*: IIF(ws_type = 'D' AND ws_trans_type = 'K', v_total_db_count_chk +  1, v_total_db_count_chk)
	IFF(ws_type = 'D' AND ws_trans_type = 'K', v_total_db_count_chk + 1, v_total_db_count_chk) AS v_total_db_count_chk,
	-- *INF*: IIF(ws_type = 'T' AND v_total_db_amount_chk != ws_total_db_amount, 'N','Y')
	IFF(ws_type = 'T' AND v_total_db_amount_chk != ws_total_db_amount, 'N', 'Y') AS v_db_amt_check,
	-- *INF*: IIF(ws_type = 'T' AND v_total_db_count_chk != ws_total_db_count, 'N','Y')
	IFF(ws_type = 'T' AND v_total_db_count_chk != ws_total_db_count, 'N', 'Y') AS v_db_cnt_check
	FROM SQ_bank_file_stage
),
FLT_Source_Rows AS (
	SELECT
	O_audit_id, 
	O_eff_from_date, 
	O_eff_to_date, 
	O_created_date, 
	O_modified_date, 
	ws_type, 
	ws_bank_nbr, 
	ws_acct_nbr, 
	ws_trans_type, 
	ws_check_nbr, 
	ws_amount, 
	ws_trans_code, 
	ws_trans_date, 
	ws_trans_status
	FROM EXP_Source
	WHERE ws_type = 'D' AND ws_trans_type = 'K'
),
EXP_Insert AS (
	SELECT
	O_audit_id,
	O_eff_from_date,
	O_eff_to_date,
	SYSDATE AS O_created_date,
	SYSDATE AS O_modified_date,
	ws_bank_nbr,
	-- *INF*: IIF(ISNULL(ltrim(rtrim(ws_bank_nbr))),'N/A',ws_bank_nbr)
	IFF(ltrim(rtrim(ws_bank_nbr)) IS NULL, 'N/A', ws_bank_nbr) AS o_ws_bank_nbr,
	ws_acct_nbr,
	-- *INF*: IIF(ISNULL(ltrim(rtrim(ws_acct_nbr))),'N/A',ws_acct_nbr)
	IFF(ltrim(rtrim(ws_acct_nbr)) IS NULL, 'N/A', ws_acct_nbr) AS o_ws_acct_nbr,
	ws_trans_type,
	ws_check_nbr,
	ws_amount,
	ws_trans_code,
	ws_trans_date,
	-- *INF*: IIF(ISNULL(ws_trans_date),TO_DATE('1/1/1800','MM/DD/YYYY'),ws_trans_date)
	IFF(ws_trans_date IS NULL, TO_DATE('1/1/1800', 'MM/DD/YYYY'), ws_trans_date) AS o_ws_trans_date,
	ws_trans_status
	FROM FLT_Source_Rows
),
claim_bank_payment AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_bank_payment
	(audit_id, eff_from_date, eff_to_date, created_date, modified_date, bank_num, acct_num, draft_num, pay_amt, pay_date)
	SELECT 
	O_audit_id AS AUDIT_ID, 
	O_eff_from_date AS EFF_FROM_DATE, 
	O_eff_to_date AS EFF_TO_DATE, 
	O_created_date AS CREATED_DATE, 
	O_modified_date AS MODIFIED_DATE, 
	o_ws_bank_nbr AS BANK_NUM, 
	o_ws_acct_nbr AS ACCT_NUM, 
	ws_check_nbr AS DRAFT_NUM, 
	ws_amount AS PAY_AMT, 
	o_ws_trans_date AS PAY_DATE
	FROM EXP_Insert
),