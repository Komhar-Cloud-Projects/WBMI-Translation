WITH
SQ_Underwriter_stage AS (
	SELECT
		uw_stage_id,
		uw_code,
		uw_first_name,
		uw_middle_name,
		uw_last_name,
		uw_suffix,
		uw_extension,
		routing_station,
		emp_id,
		EXTRACT_DATE,
		AS_OF_DATE,
		RECORD_COUNT,
		SOURCE_SYSTEM_ID
	FROM Underwriter_stage
),
exp_AGY_Insert_ARCH_UW AS (
	SELECT
	uw_stage_id,
	uw_code,
	uw_first_name,
	uw_middle_name,
	uw_last_name,
	uw_suffix,
	uw_extension,
	routing_station,
	emp_id,
	EXTRACT_DATE,
	AS_OF_DATE,
	RECORD_COUNT,
	SOURCE_SYSTEM_ID,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS OUT_AUDIT_ID
	FROM SQ_Underwriter_stage
),
TGT_Arch_underwriter_stage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.arch_underwriter_stage
	(uw_stage_id, uw_code, uw_first_name, uw_middle_name, uw_last_name, uw_suffix, uw_extension, routing_station, emp_id, EXTRACT_DATE, AS_OF_DATE, RECORD_COUNT, SOURCE_SYSTEM_ID, audit_id)
	SELECT 
	UW_STAGE_ID, 
	UW_CODE, 
	UW_FIRST_NAME, 
	UW_MIDDLE_NAME, 
	UW_LAST_NAME, 
	UW_SUFFIX, 
	UW_EXTENSION, 
	ROUTING_STATION, 
	EMP_ID, 
	EXTRACT_DATE, 
	AS_OF_DATE, 
	RECORD_COUNT, 
	SOURCE_SYSTEM_ID, 
	OUT_AUDIT_ID AS AUDIT_ID
	FROM exp_AGY_Insert_ARCH_UW
),