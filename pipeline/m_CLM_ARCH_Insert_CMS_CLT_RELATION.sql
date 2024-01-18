WITH
SQ_cms_clt_relation_stage AS (
	SELECT
		cms_clt_relation_stage_id,
		cre_claim_nbr,
		cre_seq_nbr,
		cre_client_id,
		cre_client_role_cd,
		cre_rel_to_clt_id,
		cms_party_type,
		created_ts,
		created_user_id,
		modified_ts,
		modified_user_id,
		extract_date,
		as_of_date,
		record_count,
		source_system_id
	FROM cms_clt_relation_stage
	WHERE CREATED_TS >= '@{pipeline().parameters.SELECTION_START_TS}'  OR MODIFIED_TS >= '@{pipeline().parameters.SELECTION_START_TS}'
),
EXP_CMS_CLT_RELATION_STAGE AS (
	SELECT
	cms_clt_relation_stage_id,
	cre_claim_nbr,
	cre_seq_nbr,
	cre_client_id,
	cre_client_role_cd,
	cre_rel_to_clt_id,
	cms_party_type,
	created_ts,
	created_user_id,
	modified_ts,
	modified_user_id,
	extract_date,
	as_of_date,
	record_count,
	source_system_id,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AUDIT_ID_OP
	FROM SQ_cms_clt_relation_stage
),
arch_cms_clt_relation_stage AS (
	INSERT INTO arch_cms_clt_relation_stage
	(cms_clt_relation_stage_id, cre_claim_nbr, cre_seq_nbr, cre_client_id, cre_client_role_cd, cre_rel_to_clt_id, cms_party_type, created_ts, created_user_id, modified_ts, modified_user_id, extract_date, as_of_date, record_count, source_system_id, audit_id)
	SELECT 
	CMS_CLT_RELATION_STAGE_ID, 
	CRE_CLAIM_NBR, 
	CRE_SEQ_NBR, 
	CRE_CLIENT_ID, 
	CRE_CLIENT_ROLE_CD, 
	CRE_REL_TO_CLT_ID, 
	CMS_PARTY_TYPE, 
	CREATED_TS, 
	CREATED_USER_ID, 
	MODIFIED_TS, 
	MODIFIED_USER_ID, 
	EXTRACT_DATE, 
	AS_OF_DATE, 
	RECORD_COUNT, 
	SOURCE_SYSTEM_ID, 
	AUDIT_ID_OP AS AUDIT_ID
	FROM EXP_CMS_CLT_RELATION_STAGE
),