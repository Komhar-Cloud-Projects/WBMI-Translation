WITH
SQ_Bank_file AS (

-- TODO Manual --

),
EXP_bank_file AS (
	SELECT
	WS_TYPE AS ws_type,
	WS_BANK_NBR AS IN_ws_bank_nbr,
	'04' AS o_ws_bank_nbr,
	WS_ACCT_NBR AS IN_ws_acct_nbr,
	-- *INF*: IN_ws_acct_nbr
	-- 
	-- --IIF(ws_type = 'D',IN_ws_acct_nbr,NULL)
	IN_ws_acct_nbr AS ws_acct_nbr,
	WS_TRANS_TYPE AS IN_ws_trans_type,
	-- *INF*: IIF(ws_type = 'D',IN_ws_trans_type,NULL)
	IFF(ws_type = 'D', IN_ws_trans_type, NULL) AS ws_trans_type,
	WS_CHECK_NBR AS IN_ws_check_nbr,
	-- *INF*: IIF(ws_type = 'D',IN_ws_check_nbr,NULL)
	IFF(ws_type = 'D', IN_ws_check_nbr, NULL) AS ws_check_nbr,
	WS_AMOUNT AS IN_ws_amount,
	-- *INF*: IIF(ws_type = 'D',   SUBSTR(IN_ws_amount,1,11)  || '.' ||
	-- SUBSTR(IN_ws_amount,12,2)  
	-- ,NULL)  
	-- 
	-- 
	IFF(ws_type = 'D', SUBSTR(IN_ws_amount, 1, 11) || '.' || SUBSTR(IN_ws_amount, 12, 2), NULL) AS v_ws_amount,
	-- *INF*: IIF(ws_type = 'D',TO_DECIMAL(v_ws_amount),NULL)
	IFF(ws_type = 'D', CAST(v_ws_amount AS FLOAT), NULL) AS ws_amount,
	WS_TRANS_CODE AS IN_ws_trans_code,
	-- *INF*: IIF(ws_type = 'D',IN_ws_trans_code,NULL)
	IFF(ws_type = 'D', IN_ws_trans_code, NULL) AS ws_trans_code,
	WS_TRANS_DATE AS IN_ws_trans_date,
	-- *INF*:  IIF(ws_type = 'D', TO_DATE(IN_ws_trans_date,'MMDDYY') ,NULL)  
	IFF(ws_type = 'D', TO_TIMESTAMP(IN_ws_trans_date, 'MMDDYY'), NULL) AS ws_trans_date,
	WS_CONTROL_NBR AS IN_ws_control_nbr,
	-- *INF*: IIF(ws_type = 'D',IN_ws_control_nbr,NULL)
	IFF(ws_type = 'D', IN_ws_control_nbr, NULL) AS ws_control_nbr,
	WS_TRANS_STATUS AS IN_ws_trans_status,
	-- *INF*: IIF(ws_type = 'D',IN_ws_trans_status,NULL)
	IFF(ws_type = 'D', IN_ws_trans_status, NULL) AS ws_trans_status,
	-- *INF*: IIF(ws_type = 'T',
	--  ws_type || 
	--  IN_ws_bank_nbr || IN_ws_acct_nbr || IN_ws_trans_type || IN_ws_check_nbr || IN_ws_amount || IN_ws_trans_code || IN_ws_trans_date || IN_ws_control_nbr || IN_ws_trans_status || FILLER_1
	-- ,NULL)  
	-- 
	-- 
	-- 
	-- 
	--  
	IFF(
	    ws_type = 'T',
	    ws_type || IN_ws_bank_nbr || IN_ws_acct_nbr || IN_ws_trans_type || IN_ws_check_nbr || IN_ws_amount || IN_ws_trans_code || IN_ws_trans_date || IN_ws_control_nbr || IN_ws_trans_status || FILLER_1,
	    NULL
	) AS v_trailer_str,
	-- *INF*: IIF(ws_type = 'T',   SUBSTR(v_trailer_str,15,11)  || '.' ||
	-- SUBSTR(v_trailer_str,26,2)  
	-- ,NULL)  
	-- 
	-- 
	--  
	--   
	IFF(
	    ws_type = 'T', SUBSTR(v_trailer_str, 15, 11) || '.' || SUBSTR(v_trailer_str, 26, 2), NULL
	) AS v_ws_total_cr_amount_str,
	-- *INF*: IIF(ws_type = 'T', TO_DECIMAL(v_ws_total_cr_amount_str), NULL)
	-- 
	-- 
	-- 
	IFF(ws_type = 'T', CAST(v_ws_total_cr_amount_str AS FLOAT), NULL) AS ws_total_cr_amount,
	-- *INF*: IIF(ws_type = 'T',  LTRIM(SUBSTR(v_trailer_str,  28,6 ) ,'0'),NULL)  
	-- 
	-- 
	--  
	IFF(ws_type = 'T', LTRIM(SUBSTR(v_trailer_str, 28, 6), '0'), NULL) AS v_ws_total_cr_count_str,
	-- *INF*: IIF(ws_type = 'T', TO_INTEGER(v_ws_total_cr_count_str),NULL)  
	-- 
	-- 
	-- 
	IFF(ws_type = 'T', CAST(v_ws_total_cr_count_str AS INTEGER), NULL) AS ws_total_cr_count,
	-- *INF*: IIF(ws_type = 'T',   SUBSTR(v_trailer_str,34,11)  || '.' ||
	-- SUBSTR(v_trailer_str,45,2)  
	-- ,NULL)  
	--   
	IFF(
	    ws_type = 'T', SUBSTR(v_trailer_str, 34, 11) || '.' || SUBSTR(v_trailer_str, 45, 2), NULL
	) AS v_ws_total_db_amount_str,
	-- *INF*: IIF(ws_type = 'T', TO_DECIMAL(v_ws_total_db_amount_str), NULL)
	-- 
	-- 
	--  
	IFF(ws_type = 'T', CAST(v_ws_total_db_amount_str AS FLOAT), NULL) AS ws_total_db_amount,
	-- *INF*: IIF(ws_type = 'T',   LTRIM(SUBSTR(v_trailer_str,47,6),'0')
	-- ,NULL)  
	--   
	IFF(ws_type = 'T', LTRIM(SUBSTR(v_trailer_str, 47, 6), '0'), NULL) AS v_ws_total_db_count_str,
	-- *INF*: IIF(ws_type = 'T', TO_INTEGER(v_ws_total_db_count_str),NULL)  
	IFF(ws_type = 'T', CAST(v_ws_total_db_count_str AS INTEGER), NULL) AS ws_total_db_count,
	-- *INF*: IIF(ws_type = 'T',   SUBSTR(v_trailer_str,53,6)
	-- ,NULL)  
	IFF(ws_type = 'T', SUBSTR(v_trailer_str, 53, 6), NULL) AS v_ws_processing_date_str,
	-- *INF*: IIF(ws_type = 'T', TO_DATE(v_ws_processing_date_str,'MMDDYY') ,NULL)  
	IFF(ws_type = 'T', TO_TIMESTAMP(v_ws_processing_date_str, 'MMDDYY'), NULL) AS ws_processing_date,
	-- *INF*: IIF(ws_type = 'T',   SUBSTR(v_trailer_str,59,11)  || '.' ||
	-- SUBSTR(v_trailer_str,70,2)  
	-- ,NULL)  
	IFF(
	    ws_type = 'T', SUBSTR(v_trailer_str, 59, 11) || '.' || SUBSTR(v_trailer_str, 70, 2), NULL
	) AS v_ws_cycle_to_date_amt_str,
	-- *INF*: IIF(ws_type = 'T', TO_DECIMAL(v_ws_cycle_to_date_amt_str), NULL)
	IFF(ws_type = 'T', CAST(v_ws_cycle_to_date_amt_str AS FLOAT), NULL) AS ws_cycle_to_date_amount,
	-- *INF*: IIF(ws_type = 'T', SUBSTR( v_trailer_str,72,1) ,NULL)  
	IFF(ws_type = 'T', SUBSTR(v_trailer_str, 72, 1), NULL) AS o_ws_balance_sign,
	SYSDATE AS extract_date,
	SYSDATE AS as_of_date,
	'' AS record_count,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS source_system_id,
	WS_FILLER AS FILLER_1
	FROM SQ_Bank_file
),
bank_file_stage AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.bank_file_stage;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.bank_file_stage
	(ws_type, ws_bank_nbr, ws_acct_nbr, ws_trans_type, ws_check_nbr, ws_amount, ws_trans_code, ws_trans_date, ws_control_nbr, ws_trans_status, ws_total_cr_amount, ws_total_cr_count, ws_total_db_amount, ws_total_db_count, ws_processing_date, ws_cycle_to_date_amount, ws_balance_sign, extract_date, as_of_date, record_count, source_system_id)
	SELECT 
	WS_TYPE, 
	o_ws_bank_nbr AS WS_BANK_NBR, 
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
	o_ws_balance_sign AS WS_BALANCE_SIGN, 
	EXTRACT_DATE, 
	AS_OF_DATE, 
	RECORD_COUNT, 
	SOURCE_SYSTEM_ID
	FROM EXP_bank_file
),