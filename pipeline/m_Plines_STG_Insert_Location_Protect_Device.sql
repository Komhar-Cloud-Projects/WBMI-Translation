WITH
SQ_location_protect_device AS (
	SELECT
		loc_seq,
		protect_device_type_code,
		modified_date,
		modified_user_id,
		endorsement_view_code
	FROM location_protect_device
),
EXP_LOCATION_PROTECT_DEVICE AS (
	SELECT
	loc_seq,
	protect_device_type_code,
	modified_date,
	modified_user_id,
	endorsement_view_code,
	SYSDATE AS EXTRACT_DATE,
	SYSDATE AS AS_OF_DATE,
	'' AS RECORD_COUNT,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS SOURCE_SYSTEM_ID
	FROM SQ_location_protect_device
),
location_protect_device_stage AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.location_protect_device_stage;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.location_protect_device_stage
	(loc_seq, protect_device_type_code, modified_date, modified_user_id, endorsement_view_code, extract_date, as_of_date, record_count, source_system_id)
	SELECT 
	LOC_SEQ, 
	PROTECT_DEVICE_TYPE_CODE, 
	MODIFIED_DATE, 
	MODIFIED_USER_ID, 
	ENDORSEMENT_VIEW_CODE, 
	EXTRACT_DATE AS EXTRACT_DATE, 
	AS_OF_DATE AS AS_OF_DATE, 
	RECORD_COUNT AS RECORD_COUNT, 
	SOURCE_SYSTEM_ID AS SOURCE_SYSTEM_ID
	FROM EXP_LOCATION_PROTECT_DEVICE
),