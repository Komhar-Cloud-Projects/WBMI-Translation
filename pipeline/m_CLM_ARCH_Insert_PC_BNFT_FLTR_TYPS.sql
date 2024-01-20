WITH
SQ_pc_bnft_fltr_typs_stage AS (
	SELECT p.pc_bnft_fltr_typs_stage_id, 
	p.filter_type, 
	p.description, 
	p.cov_category_code, 
	p.fin_type_cd, 
	p.modified_date, 
	p.modified_user_id, 
	p.extract_date, 
	p.source_sys_id 
	FROM
	 @{pipeline().parameters.SOURCE_TABLE_OWNER}.pc_bnft_fltr_typs_stage p
	WHERE NOT EXISTS(
	SELECT 'X'
	FROM  @{pipeline().parameters.TARGET_TABLE_OWNER}.pc_bnft_fltr_typs_stage  pp
	WHERE pp.filter_type = p.filter_type
	AND pp.description = p.description 
	AND pp.cov_category_code =  p.cov_category_code
	AND pp.fin_type_cd =  p.fin_type_cd)
),
EXP_PC_BNFT_FLTR_TYPS AS (
	SELECT
	pc_bnft_fltr_typs_stage_id,
	filter_type,
	description,
	cov_category_code,
	fin_type_cd,
	modified_date,
	modified_user_id,
	extract_date AS EXTRACT_DATE,
	source_sys_id AS SOURCE_SYSTEM_ID,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AUDIT_ID_OP
	FROM SQ_pc_bnft_fltr_typs_stage
),
arch_pc_bnft_fltr_typs_stage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.arch_pc_bnft_fltr_typs_stage
	(pc_bnft_fltr_typs_stage_id, filter_type, description, cov_category_code, fin_type_cd, modified_date, modified_user_id, extract_date, source_sys_id, audit_id)
	SELECT 
	PC_BNFT_FLTR_TYPS_STAGE_ID, 
	FILTER_TYPE, 
	DESCRIPTION, 
	COV_CATEGORY_CODE, 
	FIN_TYPE_CD, 
	MODIFIED_DATE, 
	MODIFIED_USER_ID, 
	EXTRACT_DATE AS EXTRACT_DATE, 
	SOURCE_SYSTEM_ID AS SOURCE_SYS_ID, 
	AUDIT_ID_OP AS AUDIT_ID
	FROM EXP_PC_BNFT_FLTR_TYPS
),