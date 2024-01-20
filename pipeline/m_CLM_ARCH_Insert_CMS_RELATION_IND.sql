WITH
SQ_source AS (
	SELECT
		cms_relation_ind_stage_id,
		cms_party_type,
		cms_relation_ind,
		is_individual,
		cms_relation_desc,
		cms_rel_file_code,
		created_ts,
		created_user_id,
		modified_ts,
		modified_user_id,
		extract_date,
		as_of_date,
		record_count,
		source_system_id
	FROM cms_relation_ind_stage
	WHERE CREATED_TS >= '@{pipeline().parameters.SELECTION_START_TS}'  OR MODIFIED_TS >= '@{pipeline().parameters.SELECTION_START_TS}'
),
EXP_values AS (
	SELECT
	cms_relation_ind_stage_id,
	cms_party_type,
	cms_relation_ind,
	is_individual,
	cms_relation_desc,
	cms_rel_file_code,
	created_ts,
	created_user_id,
	modified_ts,
	modified_user_id,
	extract_date,
	as_of_date,
	record_count,
	source_system_id,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AUDIT_ID
	FROM SQ_source
),
arch_cms_relation_ind_stage AS (
	INSERT INTO arch_cms_relation_ind_stage
	(cms_relation_ind_stage_id, cms_party_type, cms_relation_ind, is_individual, cms_relation_desc, cms_rel_file_code, created_ts, created_user_id, modified_ts, modified_user_id, extract_date, as_of_date, record_count, source_system_id, audit_id)
	SELECT 
	CMS_RELATION_IND_STAGE_ID, 
	CMS_PARTY_TYPE, 
	CMS_RELATION_IND, 
	IS_INDIVIDUAL, 
	CMS_RELATION_DESC, 
	CMS_REL_FILE_CODE, 
	CREATED_TS, 
	CREATED_USER_ID, 
	MODIFIED_TS, 
	MODIFIED_USER_ID, 
	EXTRACT_DATE, 
	AS_OF_DATE, 
	RECORD_COUNT, 
	SOURCE_SYSTEM_ID, 
	AUDIT_ID AS AUDIT_ID
	FROM EXP_values
),