WITH
SQ_underwriter_terr_stage AS (
	SELECT
		uw_terr_stage_id,
		rsm_id,
		territory_code,
		uw_code,
		uw_mgr_id,
		EXTRACT_DATE,
		AS_OF_DATE,
		RECORD_COUNT,
		SOURCE_SYSTEM_ID
	FROM Underwriter_terr_stage
),
exp_ARCH_Insert_UW_terr AS (
	SELECT
	uw_terr_stage_id,
	rsm_id,
	territory_code,
	uw_code,
	uw_mgr_id,
	EXTRACT_DATE,
	AS_OF_DATE,
	RECORD_COUNT,
	SOURCE_SYSTEM_ID,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS OUT_Audit_Id
	FROM SQ_underwriter_terr_stage
),
TGT_Arch_underwriter_terr_stage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.arch_underwriter_terr_stage
	(uw_terr_stage_id, rsm_id, territory_code, uw_code, uw_mgr_id, EXTRACT_DATE, AS_OF_DATE, RECORD_COUNT, SOURCE_SYSTEM_ID, audit_id)
	SELECT 
	UW_TERR_STAGE_ID, 
	RSM_ID, 
	TERRITORY_CODE, 
	UW_CODE, 
	UW_MGR_ID, 
	EXTRACT_DATE, 
	AS_OF_DATE, 
	RECORD_COUNT, 
	SOURCE_SYSTEM_ID, 
	OUT_Audit_Id AS AUDIT_ID
	FROM exp_ARCH_Insert_UW_terr
),