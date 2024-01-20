WITH
SQ_agency_underwriter_stage AS (
	SELECT
		agency_uw_stage_id,
		state_code,
		agency_num,
		insurance_line,
		uw_assistant_flag,
		uw_code,
		agency_code,
		EXTRACT_DATE,
		AS_OF_DATE,
		RECORD_COUNT,
		SOURCE_SYSTEM_ID
	FROM Agency_underwriter_stage
),
exp_AGY_Insert_Arch_Agy_UW AS (
	SELECT
	agency_uw_stage_id AS agency_uw_id,
	state_code,
	agency_num,
	insurance_line,
	uw_assistant_flag,
	uw_code,
	agency_code,
	EXTRACT_DATE,
	AS_OF_DATE,
	RECORD_COUNT,
	SOURCE_SYSTEM_ID,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS OUT_AUDIT_ID
	FROM SQ_agency_underwriter_stage
),
TGT_arch_agency_underwriter_stage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.arch_agency_underwriter_stage
	(agency_uw_stage_id, state_code, agency_num, insurance_line, uw_assistant_flag, uw_code, agency_code, EXTRACT_DATE, AS_OF_DATE, RECORD_COUNT, SOURCE_SYSTEM_ID, audit_id)
	SELECT 
	agency_uw_id AS AGENCY_UW_STAGE_ID, 
	STATE_CODE, 
	AGENCY_NUM, 
	INSURANCE_LINE, 
	UW_ASSISTANT_FLAG, 
	UW_CODE, 
	AGENCY_CODE, 
	EXTRACT_DATE, 
	AS_OF_DATE, 
	RECORD_COUNT, 
	SOURCE_SYSTEM_ID, 
	OUT_AUDIT_ID AS AUDIT_ID
	FROM exp_AGY_Insert_Arch_Agy_UW
),