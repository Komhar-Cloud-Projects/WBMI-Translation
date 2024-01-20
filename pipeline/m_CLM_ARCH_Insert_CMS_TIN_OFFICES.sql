WITH
SQ_source AS (
	SELECT
		cms_tin_offices_stage_id,
		cms_rre_id,
		office_tin_num,
		office_cd,
		office_name,
		office_mail_addr1,
		office_mail_addr2,
		office_mail_city,
		office_mail_state,
		office_mail_zip,
		office_mail_zip4,
		created_ts,
		created_user_id,
		modified_ts,
		modified_user_id,
		extract_date,
		as_of_date,
		record_count,
		source_system_id
	FROM cms_tin_offices_stage
	WHERE CREATED_TS >= '@{pipeline().parameters.SELECTION_START_TS}'  OR MODIFIED_TS >= '@{pipeline().parameters.SELECTION_START_TS}'
),
EXP_values AS (
	SELECT
	cms_tin_offices_stage_id,
	cms_rre_id,
	office_tin_num,
	office_cd,
	office_name,
	office_mail_addr1,
	office_mail_addr2,
	office_mail_city,
	office_mail_state,
	office_mail_zip,
	office_mail_zip4,
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
arch_cms_tin_offices_stage AS (
	INSERT INTO arch_cms_tin_offices_stage
	(cms_tin_offices_stage_id, cms_rre_id, office_tin_num, office_cd, office_name, office_mail_addr1, office_mail_addr2, office_mail_city, office_mail_state, office_mail_zip, office_mail_zip4, created_ts, created_user_id, modified_ts, modified_user_id, extract_date, as_of_date, record_count, source_system_id, audit_id)
	SELECT 
	CMS_TIN_OFFICES_STAGE_ID, 
	CMS_RRE_ID, 
	OFFICE_TIN_NUM, 
	OFFICE_CD, 
	OFFICE_NAME, 
	OFFICE_MAIL_ADDR1, 
	OFFICE_MAIL_ADDR2, 
	OFFICE_MAIL_CITY, 
	OFFICE_MAIL_STATE, 
	OFFICE_MAIL_ZIP, 
	OFFICE_MAIL_ZIP4, 
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