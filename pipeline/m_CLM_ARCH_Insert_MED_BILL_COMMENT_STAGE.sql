WITH
SQ_med_bill_comment_stage AS (
	SELECT
		med_bill_comment_stage_id,
		med_bill_cmnt_id,
		med_bill_id,
		comment_seq_num,
		comment_type,
		comment,
		extract_date,
		as_of_date,
		record_count,
		source_system_id
	FROM med_bill_comment_stage
),
EXP_arch_med_bill_comment_stage AS (
	SELECT
	med_bill_comment_stage_id,
	med_bill_cmnt_id,
	med_bill_id,
	comment_seq_num,
	comment_type,
	comment,
	extract_date,
	as_of_date,
	record_count,
	source_system_id,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AUDIT_ID_OP
	FROM SQ_med_bill_comment_stage
),
arch_med_bill_comment_stage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.arch_med_bill_comment_stage
	(med_bill_comment_stage_id, med_bill_cmnt_id, med_bill_id, comment_seq_num, comment_type, comment, extract_date, as_of_date, record_count, source_system_id, audit_id)
	SELECT 
	MED_BILL_COMMENT_STAGE_ID, 
	MED_BILL_CMNT_ID, 
	MED_BILL_ID, 
	COMMENT_SEQ_NUM, 
	COMMENT_TYPE, 
	COMMENT, 
	EXTRACT_DATE, 
	AS_OF_DATE, 
	RECORD_COUNT, 
	SOURCE_SYSTEM_ID, 
	AUDIT_ID_OP AS AUDIT_ID
	FROM EXP_arch_med_bill_comment_stage
),