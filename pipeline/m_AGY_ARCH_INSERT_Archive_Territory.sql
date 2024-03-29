WITH
SQ_Territory_stage AS (
	SELECT
		territory_stage_id AS territory_STAGE_id,
		RSM_ID,
		TERRITORY_CODE,
		UW_MGR_ID,
		STATE_CODE,
		TERRITORY_SYMBOL,
		TERRITORY_NAME,
		EXTRACT_DATE,
		AS_OF_DATE,
		RECORD_COUNT,
		SOURCE_SYSTEM_ID
	FROM Territory_Stage
),
exp_AGY_ARCH_Territory AS (
	SELECT
	territory_STAGE_id,
	RSM_ID,
	TERRITORY_CODE,
	UW_MGR_ID,
	STATE_CODE,
	TERRITORY_SYMBOL,
	TERRITORY_NAME,
	EXTRACT_DATE,
	AS_OF_DATE,
	RECORD_COUNT,
	SOURCE_SYSTEM_ID,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS OUT_AUDIT_ID
	FROM SQ_Territory_stage
),
TGT_Arch_territory_stage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.arch_territory_stage
	(territory_stage_id, RSM_ID, TERRITORY_CODE, UW_MGR_ID, STATE_CODE, TERRITORY_SYMBOL, TERRITORY_NAME, EXTRACT_DATE, AS_OF_DATE, RECORD_COUNT, SOURCE_SYSTEM_ID, audit_id)
	SELECT 
	territory_STAGE_id AS TERRITORY_STAGE_ID, 
	RSM_ID, 
	TERRITORY_CODE, 
	UW_MGR_ID, 
	STATE_CODE, 
	TERRITORY_SYMBOL, 
	TERRITORY_NAME, 
	EXTRACT_DATE, 
	AS_OF_DATE, 
	RECORD_COUNT, 
	SOURCE_SYSTEM_ID, 
	OUT_AUDIT_ID AS AUDIT_ID
	FROM exp_AGY_ARCH_Territory
),