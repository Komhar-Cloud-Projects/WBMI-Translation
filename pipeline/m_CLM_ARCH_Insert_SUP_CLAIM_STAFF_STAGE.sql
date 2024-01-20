WITH
SQ_sup_claim_staff_stage AS (
	SELECT
		sup_claim_staff_stage_id,
		STAFF_CODE,
		EFF_DATE,
		INITIALS,
		FIRST_NAME,
		LAST_NAME,
		POSITION_TYPE,
		PHONE,
		FAX,
		EMAIL,
		REPORT_OFFICE_CODE,
		EXP_DATE,
		OPERATOR_ID,
		WBCONNECT_USER_ID,
		MODIFIED_DATE,
		MODIFIED_USER_ID,
		extract_date,
		as_of_date,
		record_count,
		source_system_id
	FROM sup_claim_staff_stage
),
LKP_Target AS (
	SELECT
	arch_sup_claim_staff_stage_id,
	EFF_DATE,
	INITIALS,
	FIRST_NAME,
	LAST_NAME,
	POSITION_TYPE,
	PHONE,
	FAX,
	EMAIL,
	REPORT_OFFICE_CODE,
	EXP_DATE,
	OPERATOR_ID,
	WBCONNECT_USER_ID,
	MODIFIED_DATE,
	MODIFIED_USER_ID,
	STAFF_CODE
	FROM (
		SELECT 
			arch_sup_claim_staff_stage_id,
			EFF_DATE,
			INITIALS,
			FIRST_NAME,
			LAST_NAME,
			POSITION_TYPE,
			PHONE,
			FAX,
			EMAIL,
			REPORT_OFFICE_CODE,
			EXP_DATE,
			OPERATOR_ID,
			WBCONNECT_USER_ID,
			MODIFIED_DATE,
			MODIFIED_USER_ID,
			STAFF_CODE
		FROM arch_sup_claim_staff_stage
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY STAFF_CODE ORDER BY arch_sup_claim_staff_stage_id) = 1
),
EXP_Source AS (
	SELECT
	LKP_Target.arch_sup_claim_staff_stage_id,
	LKP_Target.EFF_DATE AS llkp_EFF_DATE,
	LKP_Target.INITIALS AS lkp_INITIALS,
	LKP_Target.FIRST_NAME AS lkp_FIRST_NAME,
	LKP_Target.LAST_NAME AS lkp_LAST_NAME,
	LKP_Target.POSITION_TYPE AS lkp_POSITION_TYPE,
	LKP_Target.PHONE AS lkp_PHONE,
	LKP_Target.FAX AS lkp_FAX,
	LKP_Target.EMAIL AS lkp_EMAIL,
	LKP_Target.REPORT_OFFICE_CODE AS lkp_REPORT_OFFICE_CODE,
	LKP_Target.EXP_DATE AS lkp_EXP_DATE,
	LKP_Target.OPERATOR_ID AS lkp_OPERATOR_ID,
	LKP_Target.WBCONNECT_USER_ID AS lkp_WBCONNECT_USER_ID,
	LKP_Target.MODIFIED_DATE AS lkp_MODIFIED_DATE,
	LKP_Target.MODIFIED_USER_ID AS lkp_MODIFIED_USER_ID,
	SQ_sup_claim_staff_stage.sup_claim_staff_stage_id,
	SQ_sup_claim_staff_stage.STAFF_CODE,
	SQ_sup_claim_staff_stage.EFF_DATE,
	SQ_sup_claim_staff_stage.INITIALS,
	SQ_sup_claim_staff_stage.FIRST_NAME,
	SQ_sup_claim_staff_stage.LAST_NAME,
	SQ_sup_claim_staff_stage.POSITION_TYPE,
	SQ_sup_claim_staff_stage.PHONE,
	SQ_sup_claim_staff_stage.FAX,
	SQ_sup_claim_staff_stage.EMAIL,
	SQ_sup_claim_staff_stage.REPORT_OFFICE_CODE,
	SQ_sup_claim_staff_stage.EXP_DATE,
	SQ_sup_claim_staff_stage.OPERATOR_ID,
	SQ_sup_claim_staff_stage.WBCONNECT_USER_ID,
	SQ_sup_claim_staff_stage.MODIFIED_DATE,
	SQ_sup_claim_staff_stage.MODIFIED_USER_ID,
	SQ_sup_claim_staff_stage.extract_date,
	SQ_sup_claim_staff_stage.as_of_date,
	SQ_sup_claim_staff_stage.record_count,
	SQ_sup_claim_staff_stage.source_system_id,
	-- *INF*: IIF(ISNULL(arch_sup_claim_staff_stage_id)
	-- ,'Insert'
	-- , decode(1,llkp_EFF_DATE != EFF_DATE OR 
	-- lkp_INITIALS != INITIALS OR 
	-- lkp_FIRST_NAME != FIRST_NAME  OR 
	-- lkp_LAST_NAME != LAST_NAME OR 
	-- lkp_POSITION_TYPE != POSITION_TYPE OR 
	-- lkp_PHONE != PHONE OR 
	-- lkp_FAX != FAX OR 
	-- lkp_EMAIL != EMAIL OR 
	-- lkp_REPORT_OFFICE_CODE != REPORT_OFFICE_CODE OR 
	-- lkp_EXP_DATE != EXP_DATE OR 
	-- lkp_OPERATOR_ID != OPERATOR_ID OR 
	-- lkp_WBCONNECT_USER_ID != WBCONNECT_USER_ID OR 
	-- lkp_MODIFIED_DATE != MODIFIED_DATE OR 
	-- lkp_MODIFIED_USER_ID != MODIFIED_USER_ID,
	-- 'Update', 'No Change')
	-- )
	IFF(
	    arch_sup_claim_staff_stage_id IS NULL, 'Insert',
	    decode(
	        1,
	        llkp_EFF_DATE != EFF_DATE
	    or lkp_INITIALS != INITIALS
	    or lkp_FIRST_NAME != FIRST_NAME
	    or lkp_LAST_NAME != LAST_NAME
	    or lkp_POSITION_TYPE != POSITION_TYPE
	    or lkp_PHONE != PHONE
	    or lkp_FAX != FAX
	    or lkp_EMAIL != EMAIL
	    or lkp_REPORT_OFFICE_CODE != REPORT_OFFICE_CODE
	    or lkp_EXP_DATE != EXP_DATE
	    or lkp_OPERATOR_ID != OPERATOR_ID
	    or lkp_WBCONNECT_USER_ID != WBCONNECT_USER_ID
	    or lkp_MODIFIED_DATE != MODIFIED_DATE
	    or lkp_MODIFIED_USER_ID != MODIFIED_USER_ID, 'Update',
	        'No Change'
	    )
	) AS v_change_flag,
	v_change_flag AS change_flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS Audit_ID
	FROM SQ_sup_claim_staff_stage
	LEFT JOIN LKP_Target
	ON LKP_Target.STAFF_CODE = SQ_sup_claim_staff_stage.STAFF_CODE
),
FLT_New_Records AS (
	SELECT
	sup_claim_staff_stage_id, 
	STAFF_CODE, 
	EFF_DATE, 
	INITIALS, 
	FIRST_NAME, 
	LAST_NAME, 
	POSITION_TYPE, 
	PHONE, 
	FAX, 
	EMAIL, 
	REPORT_OFFICE_CODE, 
	EXP_DATE, 
	OPERATOR_ID, 
	WBCONNECT_USER_ID, 
	MODIFIED_DATE, 
	MODIFIED_USER_ID, 
	extract_date, 
	as_of_date, 
	record_count, 
	source_system_id, 
	change_flag, 
	Audit_ID
	FROM EXP_Source
	WHERE change_flag='Update' or change_flag = 'Insert'
),
arch_sup_claim_staff_stage AS (
	INSERT INTO Shortcut_to_arch_sup_claim_staff_stage
	(sup_claim_staff_stage_id, STAFF_CODE, EFF_DATE, INITIALS, FIRST_NAME, LAST_NAME, POSITION_TYPE, PHONE, FAX, EMAIL, REPORT_OFFICE_CODE, EXP_DATE, OPERATOR_ID, WBCONNECT_USER_ID, MODIFIED_DATE, MODIFIED_USER_ID, extract_date, as_of_date, source_system_id, audit_id)
	SELECT 
	SUP_CLAIM_STAFF_STAGE_ID, 
	STAFF_CODE, 
	EFF_DATE, 
	INITIALS, 
	FIRST_NAME, 
	LAST_NAME, 
	POSITION_TYPE, 
	PHONE, 
	FAX, 
	EMAIL, 
	REPORT_OFFICE_CODE, 
	EXP_DATE, 
	OPERATOR_ID, 
	WBCONNECT_USER_ID, 
	MODIFIED_DATE, 
	MODIFIED_USER_ID, 
	EXTRACT_DATE, 
	AS_OF_DATE, 
	SOURCE_SYSTEM_ID, 
	Audit_ID AS AUDIT_ID
	FROM FLT_New_Records
),