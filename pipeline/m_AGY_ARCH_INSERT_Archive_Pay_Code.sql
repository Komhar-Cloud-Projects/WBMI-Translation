WITH
SQ_pay_code_stage AS (
	SELECT
		pay_code_stage_id,
		state_code,
		agency_num,
		pay_code,
		pay_code_exp_date,
		pay_code_eff_date,
		comm_sched_code,
		bill_pay_plan,
		agency_code,
		extract_date,
		as_of_date,
		record_count,
		source_system_id
	FROM pay_code_stage
),
EXP_pay_code_input AS (
	SELECT
	pay_code_stage_id,
	state_code,
	agency_num,
	pay_code,
	pay_code_exp_date,
	pay_code_eff_date,
	comm_sched_code,
	bill_pay_plan,
	agency_code,
	extract_date,
	as_of_date,
	record_count,
	source_system_id,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id
	FROM SQ_pay_code_stage
),
arch_pay_code_stage AS (
	INSERT INTO arch_pay_code_stage
	(pay_code_stage_id, state_code, agency_num, pay_code, pay_code_exp_date, pay_code_eff_date, comm_sched_code, bill_pay_plan, agency_code, extract_date, as_of_date, record_count, source_system_id, audit_id)
	SELECT 
	PAY_CODE_STAGE_ID, 
	STATE_CODE, 
	AGENCY_NUM, 
	PAY_CODE, 
	PAY_CODE_EXP_DATE, 
	PAY_CODE_EFF_DATE, 
	COMM_SCHED_CODE, 
	BILL_PAY_PLAN, 
	AGENCY_CODE, 
	EXTRACT_DATE, 
	AS_OF_DATE, 
	RECORD_COUNT, 
	SOURCE_SYSTEM_ID, 
	AUDIT_ID
	FROM EXP_pay_code_input
),