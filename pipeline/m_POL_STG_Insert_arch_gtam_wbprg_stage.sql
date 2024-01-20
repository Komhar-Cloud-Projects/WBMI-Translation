WITH
SQ_gtam_wbprg_stage AS (
	SELECT
		gtam_wbprg_stage_id,
		prog_id,
		exp_date,
		prog_name,
		extract_date,
		as_of_date,
		rcrd_count,
		source_sys_id
	FROM gtam_wbprg_stage
),
EXPTRANS AS (
	SELECT
	gtam_wbprg_stage_id,
	prog_id,
	exp_date,
	prog_name,
	extract_date,
	as_of_date,
	rcrd_count,
	source_sys_id,
	@{pipeline().parameters.AUDIT_ID} AS audit_id
	FROM SQ_gtam_wbprg_stage
),
arch_gtam_wbprg_stage AS (
	INSERT INTO arch_gtam_wbprg_stage
	(gtam_wbprg_stage_id, prog_id, exp_date, prog_name, extract_date, as_of_date, source_sys_id, audit_id)
	SELECT 
	GTAM_WBPRG_STAGE_ID, 
	PROG_ID, 
	EXP_DATE, 
	PROG_NAME, 
	EXTRACT_DATE, 
	AS_OF_DATE, 
	SOURCE_SYS_ID, 
	AUDIT_ID
	FROM EXPTRANS
),