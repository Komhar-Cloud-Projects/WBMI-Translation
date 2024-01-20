WITH
SQ_client_uct_stage AS (
	SELECT
		client_uct_stage_id,
		cicu_view_nm,
		ctr_nbr_cd,
		cicu_uct_cd,
		clt_spt_usage_cd,
		cicu_uct_des,
		extract_date,
		as_of_date,
		record_count,
		source_system_id
	FROM client_uct_stage
),
EXP_AUDIT_FIELDS AS (
	SELECT
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AUDIT_ID_OP,
	client_uct_stage_id,
	cicu_view_nm,
	ctr_nbr_cd,
	cicu_uct_cd,
	clt_spt_usage_cd,
	cicu_uct_des,
	extract_date,
	as_of_date,
	record_count,
	source_system_id
	FROM SQ_client_uct_stage
),
arch_client_uct_stage AS (
	INSERT INTO arch_client_uct_stage
	(client_uct_stage_id, cicu_view_nm, ctr_nbr_cd, cicu_uct_cd, clt_spt_usage_cd, cicu_uct_des, extract_date, as_of_date, record_count, source_system_id, audit_id)
	SELECT 
	CLIENT_UCT_STAGE_ID, 
	CICU_VIEW_NM, 
	CTR_NBR_CD, 
	CICU_UCT_CD, 
	CLT_SPT_USAGE_CD, 
	CICU_UCT_DES, 
	EXTRACT_DATE, 
	AS_OF_DATE, 
	RECORD_COUNT, 
	SOURCE_SYSTEM_ID, 
	AUDIT_ID_OP AS AUDIT_ID
	FROM EXP_AUDIT_FIELDS
),