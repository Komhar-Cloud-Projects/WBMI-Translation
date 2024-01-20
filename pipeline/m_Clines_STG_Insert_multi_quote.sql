WITH
SQ_multi_quote AS (
	SELECT
		quote_id,
		commssn,
		irpm,
		multi_building_bus_class_id,
		interstate_risk_id_num,
		risk_id_num_status_id,
		created_user_id,
		created_date,
		modified_user_id,
		modified_date
	FROM multi_quote
),
EXP_Values AS (
	SELECT
	quote_id,
	commssn,
	irpm,
	multi_building_bus_class_id,
	interstate_risk_id_num,
	risk_id_num_status_id,
	created_user_id,
	created_date,
	modified_user_id,
	modified_date,
	SYSDATE AS extract_date,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS source_system_id
	FROM SQ_multi_quote
),
multi_quote_cl_stage AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.multi_quote_cl_stage;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.multi_quote_cl_stage
	(quote_id, commssn, irpm, multi_building_bus_class_id, interstate_risk_id_num, risk_id_num_status_id, created_user_id, created_date, modified_user_id, modified_date, extract_date, source_system_id)
	SELECT 
	QUOTE_ID, 
	COMMSSN, 
	IRPM, 
	MULTI_BUILDING_BUS_CLASS_ID, 
	INTERSTATE_RISK_ID_NUM, 
	RISK_ID_NUM_STATUS_ID, 
	CREATED_USER_ID, 
	CREATED_DATE, 
	MODIFIED_USER_ID, 
	MODIFIED_DATE, 
	EXTRACT_DATE, 
	SOURCE_SYSTEM_ID
	FROM EXP_Values
),