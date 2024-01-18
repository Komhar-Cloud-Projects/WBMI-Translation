WITH
SQ_SUP_BENEFIT_TYPE_STAGE AS (
	SELECT
		sup_benefit_type_id AS SUP_BENEFIT_TYPE_ID,
		code AS CODE,
		descript AS DESCRIPT,
		modified_date AS MODIFIED_DATE,
		modified_user_id AS MODIFIED_USER_ID,
		fin_type_cd,
		cause_of_loss,
		filter_type,
		extract_date AS EXTRACT_DATE,
		as_of_date AS AS_OF_DATE,
		record_count AS RECORD_COUNT,
		source_system_id AS SOURCE_SYSTEM_ID
	FROM SUP_BENEFIT_TYPE_STAGE
),
EXP_SUP_BENEFIT_TYPE_STAGE AS (
	SELECT
	SUP_BENEFIT_TYPE_ID,
	CODE,
	DESCRIPT,
	MODIFIED_DATE,
	MODIFIED_USER_ID,
	fin_type_cd,
	cause_of_loss,
	filter_type,
	EXTRACT_DATE,
	AS_OF_DATE,
	RECORD_COUNT,
	SOURCE_SYSTEM_ID,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AUDIT_ID_OP
	FROM SQ_SUP_BENEFIT_TYPE_STAGE
),
ARCH_SUP_BENEFIT_TYPE_STAGE AS (
	INSERT INTO ARCH_SUP_BENEFIT_TYPE_STAGE
	(sup_benefit_type_id, code, descript, modified_date, modified_user_id, fin_type_cd, cause_of_loss, filter_type, extract_date, as_of_date, record_count, source_system_id, audit_id)
	SELECT 
	SUP_BENEFIT_TYPE_ID AS SUP_BENEFIT_TYPE_ID, 
	CODE AS CODE, 
	DESCRIPT AS DESCRIPT, 
	MODIFIED_DATE AS MODIFIED_DATE, 
	MODIFIED_USER_ID AS MODIFIED_USER_ID, 
	FIN_TYPE_CD, 
	CAUSE_OF_LOSS, 
	FILTER_TYPE, 
	EXTRACT_DATE AS EXTRACT_DATE, 
	AS_OF_DATE AS AS_OF_DATE, 
	RECORD_COUNT AS RECORD_COUNT, 
	SOURCE_SYSTEM_ID AS SOURCE_SYSTEM_ID, 
	AUDIT_ID_OP AS AUDIT_ID
	FROM EXP_SUP_BENEFIT_TYPE_STAGE
),