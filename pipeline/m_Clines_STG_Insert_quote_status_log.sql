WITH
SQ_quote_status_log AS (
	SELECT
		quote_status_log_id,
		quote_id,
		quote_status_id,
		quote_status_date_time,
		note
	FROM quote_status_log
),
EXP_Values AS (
	SELECT
	quote_status_log_id,
	quote_id,
	quote_status_id,
	quote_status_date_time,
	note,
	SYSDATE AS extract_date,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS source_system_id
	FROM SQ_quote_status_log
),
quote_status_log_cl_stage AS (
	TRUNCATE TABLE @{pipeline().parameters.SOURCE_TABLE_OWNER}.quote_status_log_cl_stage;
	INSERT INTO @{pipeline().parameters.SOURCE_TABLE_OWNER}.quote_status_log_cl_stage
	(quote_status_log_id, quote_id, quote_status_id, quote_status_date_time, note, extract_date, source_system_id)
	SELECT 
	QUOTE_STATUS_LOG_ID, 
	QUOTE_ID, 
	QUOTE_STATUS_ID, 
	QUOTE_STATUS_DATE_TIME, 
	NOTE, 
	EXTRACT_DATE, 
	SOURCE_SYSTEM_ID
	FROM EXP_Values
),