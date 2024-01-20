WITH
SQ_gtam_acr05a_stage AS (
	SELECT
		acr05a_STAGE_ID,
		TABLE_FLD,
		KEY_LEN,
		LOCATION,
		MASTER_COMPANY_NAME,
		MAJOR_PERIL,
		DATA_LEN,
		LONG_ALPHABETIC_DESCRIPTION,
		SHORT_ALPHABETIC_DESCRIPTION,
		EXTRACT_DATE,
		AS_OF_DATE,
		RECORD_COUNT,
		SOURCE_SYSTEM_ID
	FROM gtam_acr05a_stage
),
EXP_arch_GTAM_acr05a_stage AS (
	SELECT
	acr05a_STAGE_ID,
	TABLE_FLD,
	KEY_LEN,
	LOCATION,
	MASTER_COMPANY_NAME,
	MAJOR_PERIL,
	DATA_LEN,
	LONG_ALPHABETIC_DESCRIPTION,
	SHORT_ALPHABETIC_DESCRIPTION,
	EXTRACT_DATE,
	AS_OF_DATE,
	RECORD_COUNT,
	SOURCE_SYSTEM_ID,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AUDIT_ID_OP
	FROM SQ_gtam_acr05a_stage
),
arch_gtam_acr05a_stage AS (
	INSERT INTO arch_gtam_acr05a_stage
	(acr05a_STAGE_ID, TABLE_FLD, KEY_LEN, LOCATION, MASTER_COMPANY_NAME, MAJOR_PERIL, DATA_LEN, LONG_ALPHABETIC_DESCRIPTION, SHORT_ALPHABETIC_DESCRIPTION, EXTRACT_DATE, AS_OF_DATE, RECORD_COUNT, SOURCE_SYSTEM_ID, audit)
	SELECT 
	ACR05A_STAGE_ID, 
	TABLE_FLD, 
	KEY_LEN, 
	LOCATION, 
	MASTER_COMPANY_NAME, 
	MAJOR_PERIL, 
	DATA_LEN, 
	LONG_ALPHABETIC_DESCRIPTION, 
	SHORT_ALPHABETIC_DESCRIPTION, 
	EXTRACT_DATE, 
	AS_OF_DATE, 
	RECORD_COUNT, 
	SOURCE_SYSTEM_ID, 
	AUDIT_ID_OP AS AUDIT
	FROM EXP_arch_GTAM_acr05a_stage
),