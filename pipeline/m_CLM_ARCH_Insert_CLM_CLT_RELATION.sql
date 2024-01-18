WITH
SQ_clm_clt_relation_stage AS (
	SELECT
		clm_clt_relation_stage_id,
		cre_claim_nbr,
		cre_seq_nbr,
		cre_client_id,
		cre_client_role_cd,
		cre_rel_to_clt_id,
		cre_rel_to_role_cd,
		cre_object_type_cd,
		cre_object_seq_nbr,
		extract_date,
		as_of_date,
		record_count,
		source_system_id
	FROM clm_clt_relation_stage
),
EXP_CLM_CLT_RELATION_STAGE AS (
	SELECT
	clm_clt_relation_stage_id,
	cre_claim_nbr,
	cre_seq_nbr,
	cre_client_id,
	cre_client_role_cd,
	cre_rel_to_clt_id,
	cre_rel_to_role_cd,
	cre_object_type_cd,
	cre_object_seq_nbr,
	extract_date,
	as_of_date,
	record_count,
	source_system_id,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AUDIT_ID_OP
	FROM SQ_clm_clt_relation_stage
),
arch_clm_clt_relation_stage AS (
	INSERT INTO arch_clm_clt_relation_stage
	(clm_clt_relation_stage_id, cre_claim_nbr, cre_seq_nbr, cre_client_id, cre_client_role_cd, cre_rel_to_clt_id, cre_rel_to_role_cd, cre_object_type_cd, cre_object_seq_nbr, extract_date, as_of_date, record_count, source_system_id, audit_id)
	SELECT 
	CLM_CLT_RELATION_STAGE_ID, 
	CRE_CLAIM_NBR, 
	CRE_SEQ_NBR, 
	CRE_CLIENT_ID, 
	CRE_CLIENT_ROLE_CD, 
	CRE_REL_TO_CLT_ID, 
	CRE_REL_TO_ROLE_CD, 
	CRE_OBJECT_TYPE_CD, 
	CRE_OBJECT_SEQ_NBR, 
	EXTRACT_DATE, 
	AS_OF_DATE, 
	RECORD_COUNT, 
	SOURCE_SYSTEM_ID, 
	AUDIT_ID_OP AS AUDIT_ID
	FROM EXP_CLM_CLT_RELATION_STAGE
),