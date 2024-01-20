WITH
SQ_Underwriter_mgr_stage AS (
	SELECT
		uw_mgr_stage_id,
		uw_mgr_id,
		uw_mgr_first_name,
		uw_mgr_middle_name,
		uw_mgr_last_name,
		uw_mgr_suffix,
		routing_station,
		EXTRACT_DATE,
		AS_OF_DATE,
		RECORD_COUNT,
		SOURCE_SYSTEM_ID
	FROM Underwriter_mgr_stage
),
exp_ARHC_Insert_UW_Mgr AS (
	SELECT
	uw_mgr_stage_id,
	uw_mgr_id,
	uw_mgr_first_name,
	uw_mgr_middle_name,
	uw_mgr_last_name,
	uw_mgr_suffix,
	routing_station,
	EXTRACT_DATE,
	AS_OF_DATE,
	RECORD_COUNT,
	SOURCE_SYSTEM_ID,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS OUT_AUDIT_ID
	FROM SQ_Underwriter_mgr_stage
),
TGT_Arch_underwriter_mgr_stage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.arch_underwriter_mgr_stage
	(uw_mgr_stage_id, uw_mgr_id, uw_mgr_first_name, uw_mgr_middle_name, uw_mgr_last_name, uw_mgr_suffix, routing_station, EXTRACT_DATE, AS_OF_DATE, RECORD_COUNT, SOURCE_SYSTEM_ID, audit_id)
	SELECT 
	UW_MGR_STAGE_ID, 
	UW_MGR_ID, 
	UW_MGR_FIRST_NAME, 
	UW_MGR_MIDDLE_NAME, 
	UW_MGR_LAST_NAME, 
	UW_MGR_SUFFIX, 
	ROUTING_STATION, 
	EXTRACT_DATE, 
	AS_OF_DATE, 
	RECORD_COUNT, 
	SOURCE_SYSTEM_ID, 
	OUT_AUDIT_ID AS AUDIT_ID
	FROM exp_ARHC_Insert_UW_Mgr
),