WITH
SQ_med_bill_code_stage AS (
	SELECT
		med_code_bill_code_stage_id,
		med_bill_code_id,
		med_bill_id,
		med_bill_serv_id,
		code_type,
		code,
		descript,
		code_date,
		extract_date,
		as_of_date,
		record_count,
		source_system_id
	FROM med_bill_code_stage
),
EXP_arch_med_bill_code_stage AS (
	SELECT
	med_code_bill_code_stage_id,
	med_bill_code_id,
	med_bill_id,
	med_bill_serv_id,
	code_type,
	code,
	descript,
	code_date,
	extract_date,
	as_of_date,
	record_count,
	source_system_id,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AUDIT_ID_OP
	FROM SQ_med_bill_code_stage
),
arch_med_bill_code_stage AS (
	INSERT INTO arch_med_bill_code_stage
	(med_code_bill_code_stage_id, med_bill_code_id, med_bill_id, med_bill_serv_id, code_type, code, descript, code_date, extract_date, as_of_date, record_count, source_system_id, audit_id)
	SELECT 
	MED_CODE_BILL_CODE_STAGE_ID, 
	MED_BILL_CODE_ID, 
	MED_BILL_ID, 
	MED_BILL_SERV_ID, 
	CODE_TYPE, 
	CODE, 
	DESCRIPT, 
	CODE_DATE, 
	EXTRACT_DATE, 
	AS_OF_DATE, 
	RECORD_COUNT, 
	SOURCE_SYSTEM_ID, 
	AUDIT_ID_OP AS AUDIT_ID
	FROM EXP_arch_med_bill_code_stage
),