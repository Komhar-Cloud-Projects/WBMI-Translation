WITH
SQ_GTAM_sc102_stage AS (
	SELECT
		SC102_stage_ID,
		TABLE_FLD,
		KEY_LEN,
		LOCATION,
		MASTER_COMPANY_NUMBER,
		RESERVE_CATEGORY,
		DATA_LEN,
		RESERVE_CATEGORY_DESCRIPTION,
		EXTRACT_DATE,
		AS_OF_DATE,
		RECORD_COUNT,
		SOURCE_SYSTEM_ID
	FROM GTAM_sc102_stage
),
EXP_arch_GTAM_sc102_stage AS (
	SELECT
	SC102_stage_ID,
	TABLE_FLD,
	KEY_LEN,
	LOCATION,
	MASTER_COMPANY_NUMBER,
	RESERVE_CATEGORY,
	DATA_LEN,
	RESERVE_CATEGORY_DESCRIPTION,
	EXTRACT_DATE,
	AS_OF_DATE,
	RECORD_COUNT,
	SOURCE_SYSTEM_ID,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AUDIT_ID_OP
	FROM SQ_GTAM_sc102_stage
),
arch_GTAM_sc102_stage AS (
	INSERT INTO arch_GTAM_sc102_stage
	(SC102_stage_ID, TABLE_FLD, KEY_LEN, LOCATION, MASTER_COMPANY_NUMBER, RESERVE_CATEGORY, DATA_LEN, RESERVE_CATEGORY_DESCRIPTION, EXTRACT_DATE, AS_OF_DATE, RECORD_COUNT, SOURCE_SYSTEM_ID, audit_id)
	SELECT 
	SC102_STAGE_ID, 
	TABLE_FLD, 
	KEY_LEN, 
	LOCATION, 
	MASTER_COMPANY_NUMBER, 
	RESERVE_CATEGORY, 
	DATA_LEN, 
	RESERVE_CATEGORY_DESCRIPTION, 
	EXTRACT_DATE, 
	AS_OF_DATE, 
	RECORD_COUNT, 
	SOURCE_SYSTEM_ID, 
	AUDIT_ID_OP AS AUDIT_ID
	FROM EXP_arch_GTAM_sc102_stage
),