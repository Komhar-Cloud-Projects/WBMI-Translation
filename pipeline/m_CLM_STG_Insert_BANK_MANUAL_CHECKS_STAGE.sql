WITH
SQ_Shortcut_to_Manual_Checks AS (

-- TODO Manual --

),
EXP_BANK_MANUAL_CHECK_FILE AS (
	SELECT
	'D' AS WS_TYPE,
	'04' AS WS_BANK_NBR,
	ws_acct_nbr AS WS_ACCT_NBR,
	'K' AS WS_TRANS_TYPE,
	ws_check_nbr AS WS_CHECK_NBR,
	ws_amount AS WS_AMOUNT,
	'481' AS WS_TRANS_CODE,
	ws_trans_date AS IN_WS_TRANS_DATE,
	-- *INF*: TO_DATE(IN_WS_TRANS_DATE,'MM-DD-YYYY')
	TO_TIMESTAMP(IN_WS_TRANS_DATE, 'MM-DD-YYYY') AS O_WS_TRANS_DATE,
	'U' AS WS_TRANS_STATUS
	FROM SQ_Shortcut_to_Manual_Checks
),
bank_file_stage AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.bank_file_stage;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.bank_file_stage
	(ws_type, ws_bank_nbr, ws_acct_nbr, ws_trans_type, ws_check_nbr, ws_amount, ws_trans_code, ws_trans_date, ws_trans_status)
	SELECT 
	WS_TYPE AS WS_TYPE, 
	WS_BANK_NBR AS WS_BANK_NBR, 
	WS_ACCT_NBR AS WS_ACCT_NBR, 
	WS_TRANS_TYPE AS WS_TRANS_TYPE, 
	WS_CHECK_NBR AS WS_CHECK_NBR, 
	WS_AMOUNT AS WS_AMOUNT, 
	WS_TRANS_CODE AS WS_TRANS_CODE, 
	O_WS_TRANS_DATE AS WS_TRANS_DATE, 
	WS_TRANS_STATUS AS WS_TRANS_STATUS
	FROM EXP_BANK_MANUAL_CHECK_FILE
),