WITH
SQ_gtam_wb_region_stage AS (
	SELECT
		gtam_wb_region_stage_id,
		agency_code,
		bus_unit_ind,
		uw_mgr_name_routing_station,
		uw_mgr_region,
		extract_date,
		as_of_date,
		record_count,
		source_system_id
	FROM gtam_wb_region_stage
	WHERE gtam_wb_region_stage.extract_date >= '@{pipeline().parameters.SELECTION_START_TS}' 
	AND gtam_wb_region_stage.extract_date <= '@{pipeline().parameters.SELECTION_END_TS}'
),
EXP_arch_GTAM_wb_region_stage AS (
	SELECT
	gtam_wb_region_stage_id,
	agency_code,
	bus_unit_ind,
	uw_mgr_name_routing_station,
	uw_mgr_region,
	extract_date AS EXTRACT_DATE,
	as_of_date AS AS_OF_DATE,
	record_count AS RECORD_COUNT,
	source_system_id AS SOURCE_SYSTEM_ID,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AUDIT_ID_OP
	FROM SQ_gtam_wb_region_stage
),
arch_gtam_wb_region_stage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.arch_gtam_wb_region_stage
	(gtam_wb_region_stage_id, agency_code, bus_unit_ind, uw_mgr_name_routing_station, uw_mgr_region, extract_date, as_of_date, record_count, source_system_id, audit_id)
	SELECT 
	GTAM_WB_REGION_STAGE_ID, 
	AGENCY_CODE, 
	BUS_UNIT_IND, 
	UW_MGR_NAME_ROUTING_STATION, 
	UW_MGR_REGION, 
	EXTRACT_DATE AS EXTRACT_DATE, 
	AS_OF_DATE AS AS_OF_DATE, 
	RECORD_COUNT AS RECORD_COUNT, 
	SOURCE_SYSTEM_ID AS SOURCE_SYSTEM_ID, 
	AUDIT_ID_OP AS AUDIT_ID
	FROM EXP_arch_GTAM_wb_region_stage
),