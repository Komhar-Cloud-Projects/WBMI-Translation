WITH
SQ_gtam_wbsiccod_stage AS (
	SELECT
		gtam_wbsiccod_stage_id,
		sic_code_number,
		sic_code_description,
		extract_date,
		as_of_date,
		record_count,
		source_system_id
	FROM gtam_wbsiccod_stage
),
EXP_arch_GTAM_wbcomsch_stage AS (
	SELECT
	gtam_wbsiccod_stage_id,
	sic_code_number,
	sic_code_description,
	extract_date AS EXTRACT_DATE,
	as_of_date AS AS_OF_DATE,
	record_count AS RECORD_COUNT,
	source_system_id AS SOURCE_SYSTEM_ID,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AUDIT_ID_OP
	FROM SQ_gtam_wbsiccod_stage
),
arch_gtam_wbsiccod_stage AS (
	INSERT INTO arch_gtam_wbsiccod_stage
	(gtam_wbsiccod_stage_id, sic_code_number, sic_code_description, extract_date, as_of_date, record_count, source_system_id, audit_id)
	SELECT 
	GTAM_WBSICCOD_STAGE_ID, 
	SIC_CODE_NUMBER, 
	SIC_CODE_DESCRIPTION, 
	EXTRACT_DATE AS EXTRACT_DATE, 
	AS_OF_DATE AS AS_OF_DATE, 
	RECORD_COUNT AS RECORD_COUNT, 
	SOURCE_SYSTEM_ID AS SOURCE_SYSTEM_ID, 
	AUDIT_ID_OP AS AUDIT_ID
	FROM EXP_arch_GTAM_wbcomsch_stage
),