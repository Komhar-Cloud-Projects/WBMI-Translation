WITH
SQ_CLAIM_SUPPORT_01_STAGE AS (
	SELECT
		CLAIM_SUPPORT_01_ID,
		CS01_TABLE_ID,
		CS01_TABLE_SEQ_NBR,
		CS01_CODE,
		CS01_CODE_DES,
		EXTRACT_DATE,
		AS_OF_DATE,
		RECORD_COUNT,
		SOURCE_SYSTEM_ID
	FROM CLAIM_SUPPORT_01_STAGE
),
EXP_CLAIM_SUPPORT_01_STAGE AS (
	SELECT
	CLAIM_SUPPORT_01_ID,
	CS01_TABLE_ID,
	CS01_TABLE_SEQ_NBR,
	CS01_CODE,
	CS01_CODE_DES,
	EXTRACT_DATE,
	AS_OF_DATE,
	RECORD_COUNT,
	SOURCE_SYSTEM_ID,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AUDIT_ID_OP
	FROM SQ_CLAIM_SUPPORT_01_STAGE
),
ARCH_CLAIM_SUPPORT_01_STAGE AS (
	INSERT INTO ARCH_CLAIM_SUPPORT_01_STAGE
	(CLAIM_SUPPORT_01_ID, CS01_TABLE_ID, CS01_TABLE_SEQ_NBR, CS01_CODE, CS01_CODE_DES, EXTRACT_DATE, AS_OF_DATE, RECORD_COUNT, SOURCE_SYSTEM_ID, AUDIT_ID)
	SELECT 
	CLAIM_SUPPORT_01_ID, 
	CS01_TABLE_ID, 
	CS01_TABLE_SEQ_NBR, 
	CS01_CODE, 
	CS01_CODE_DES, 
	EXTRACT_DATE, 
	AS_OF_DATE, 
	RECORD_COUNT, 
	SOURCE_SYSTEM_ID, 
	AUDIT_ID_OP AS AUDIT_ID
	FROM EXP_CLAIM_SUPPORT_01_STAGE
),