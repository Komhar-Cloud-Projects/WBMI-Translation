WITH
SQ_pc_benefit_filter_stage AS (
	SELECT 
	pc_stage.pc_bnft_filter_stage_id, 
	pc_stage.filter_type, 
	pc_stage.benefit_code, 
	pc_stage.sort_order, 
	pc_stage.extract_date, 
	pc_stage.source_sys_id 
	FROM
	   @{pipeline().parameters.SOURCE_TABLE_OWNER}.pc_benefit_filter_stage   pc_stage
	WHERE NOT EXISTS
	(SELECT 'X' 
	FROM   @{pipeline().parameters.TARGET_TABLE_OWNER}.ARCH_PC_BENEFIT_FILTER_STAGE  pc_arch 
	WHERE  pc_stage.filter_type          = pc_arch.filter_type
	   AND      pc_stage.benefit_code   = pc_arch.benefit_code
	   AND     pc_stage.sort_order         = pc_arch.sort_order )
),
EXP_ARCH_PC_BENEFIT_FILTER AS (
	SELECT
	pc_bnft_filter_stage_id,
	filter_type,
	benefit_code,
	sort_order,
	extract_date AS EXTRACT_DATE,
	source_sys_id AS SOURCE_SYSTEM_ID,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AUDIT_ID_OP
	FROM SQ_pc_benefit_filter_stage
),
arch_pc_benefit_filter_stage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.arch_pc_benefit_filter_stage
	(pc_bnft_filter_stage_id, filter_type, benefit_code, sort_order, extract_date, source_sys_id, audit_id)
	SELECT 
	PC_BNFT_FILTER_STAGE_ID, 
	FILTER_TYPE, 
	BENEFIT_CODE, 
	SORT_ORDER, 
	EXTRACT_DATE AS EXTRACT_DATE, 
	SOURCE_SYSTEM_ID AS SOURCE_SYS_ID, 
	AUDIT_ID_OP AS AUDIT_ID
	FROM EXP_ARCH_PC_BENEFIT_FILTER
),