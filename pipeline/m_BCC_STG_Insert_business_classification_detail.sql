WITH
SQ_business_classification_detail AS (
	SELECT
		bus_class_det_id,
		bus_class_code,
		bus_class_descript,
		bus_seg_id,
		strtgc_bus_unit_id,
		approval_status_id,
		approval_status_chg_id,
		short_descript,
		bus_rule_descript,
		internal_reports_descript,
		created_user_id,
		created_date,
		modified_user_id,
		modified_date,
		eff_date,
		exp_date
	FROM business_classification_detail
),
EXP_Values AS (
	SELECT
	bus_class_det_id,
	bus_class_code,
	bus_class_descript,
	bus_seg_id,
	strtgc_bus_unit_id,
	approval_status_id,
	approval_status_chg_id,
	short_descript,
	bus_rule_descript,
	internal_reports_descript,
	created_user_id,
	created_date,
	modified_user_id,
	modified_date,
	eff_date,
	exp_date,
	SYSDATE AS EXTRACT_DATE,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS SOURCE_SYSTEM_ID
	FROM SQ_business_classification_detail
),
business_classification_detail_bcc_stage AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.business_classification_detail_bcc_stage;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.business_classification_detail_bcc_stage
	(bus_class_det_id, bus_class_code, bus_class_descript, bus_seg_id, strtgc_bus_unit_id, approval_status_id, approval_status_chg_id, short_descript, bus_rule_descript, internal_reports_descript, created_user_id, created_date, modified_user_id, modified_date, eff_date, exp_date, extract_date, source_system_id)
	SELECT 
	BUS_CLASS_DET_ID, 
	BUS_CLASS_CODE, 
	BUS_CLASS_DESCRIPT, 
	BUS_SEG_ID, 
	STRTGC_BUS_UNIT_ID, 
	APPROVAL_STATUS_ID, 
	APPROVAL_STATUS_CHG_ID, 
	SHORT_DESCRIPT, 
	BUS_RULE_DESCRIPT, 
	INTERNAL_REPORTS_DESCRIPT, 
	CREATED_USER_ID, 
	CREATED_DATE, 
	MODIFIED_USER_ID, 
	MODIFIED_DATE, 
	EFF_DATE, 
	EXP_DATE, 
	EXTRACT_DATE AS EXTRACT_DATE, 
	SOURCE_SYSTEM_ID AS SOURCE_SYSTEM_ID
	FROM EXP_Values
),