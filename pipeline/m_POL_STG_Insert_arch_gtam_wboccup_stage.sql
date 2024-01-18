WITH
SQ_gtam_wboccup_stage AS (
	SELECT
		gtam_wboccup_stage_id,
		business_class_code,
		classification_of_business,
		extract_date,
		as_of_date,
		record_count,
		source_system_id
	FROM gtam_wboccup_stage
),
LKP_arch_gtam_wboccup_stage AS (
	SELECT
	arch_gtam_wboccup_stage_id,
	gtam_wboccup_stage_id,
	classification_of_business,
	in_business_class_code,
	business_class_code
	FROM (
		SELECT arch_gtam_wboccup_stage.arch_gtam_wboccup_stage_id as arch_gtam_wboccup_stage_id, arch_gtam_wboccup_stage.gtam_wboccup_stage_id as gtam_wboccup_stage_id, arch_gtam_wboccup_stage.classification_of_business as classification_of_business, arch_gtam_wboccup_stage.business_class_code as business_class_code FROM arch_gtam_wboccup_stage
		where 	arch_gtam_wboccup_stage.arch_gtam_wboccup_stage_id In
			(Select max(arch_gtam_wboccup_stage_id) from arch_gtam_wboccup_stage b
			group by b.business_class_code)
		order by arch_gtam_wboccup_stage.business_class_code--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY business_class_code ORDER BY arch_gtam_wboccup_stage_id) = 1
),
EXP_arch_wboccup_stage AS (
	SELECT
	SQ_gtam_wboccup_stage.gtam_wboccup_stage_id,
	SQ_gtam_wboccup_stage.business_class_code,
	SQ_gtam_wboccup_stage.classification_of_business,
	SQ_gtam_wboccup_stage.extract_date AS EXTRACT_DATE,
	SQ_gtam_wboccup_stage.as_of_date AS AS_OF_DATE,
	SQ_gtam_wboccup_stage.record_count AS RECORD_COUNT,
	SQ_gtam_wboccup_stage.source_system_id AS SOURCE_SYSTEM_ID,
	LKP_arch_gtam_wboccup_stage.arch_gtam_wboccup_stage_id AS old_arch_gtam_wboccup_stage_id,
	LKP_arch_gtam_wboccup_stage.classification_of_business AS old_classification_of_business,
	-- *INF*: iif(isnull(old_arch_gtam_wboccup_stage_id), 'NEW',
	--      iif((old_classification_of_business<>classification_of_business),'UPDATE', 'NOCHANGE'))
	IFF(
	    old_arch_gtam_wboccup_stage_id IS NULL, 'NEW',
	    IFF(
	        (old_classification_of_business <> classification_of_business), 'UPDATE', 'NOCHANGE'
	    )
	) AS v_Changed_Flag,
	v_Changed_Flag AS Changed_Flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AUDIT_ID
	FROM SQ_gtam_wboccup_stage
	LEFT JOIN LKP_arch_gtam_wboccup_stage
	ON LKP_arch_gtam_wboccup_stage.business_class_code = SQ_gtam_wboccup_stage.business_class_code
),
FIL_Inserts AS (
	SELECT
	gtam_wboccup_stage_id, 
	business_class_code, 
	classification_of_business, 
	EXTRACT_DATE, 
	AS_OF_DATE, 
	RECORD_COUNT, 
	SOURCE_SYSTEM_ID, 
	Changed_Flag, 
	AUDIT_ID
	FROM EXP_arch_wboccup_stage
	WHERE Changed_Flag = 'NEW' OR Changed_Flag = 'UPDATE'
),
arch_gtam_wboccup_stage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.arch_gtam_wboccup_stage
	(gtam_wboccup_stage_id, business_class_code, classification_of_business, extract_date, as_of_date, record_count, source_system_id, audit_id)
	SELECT 
	GTAM_WBOCCUP_STAGE_ID, 
	BUSINESS_CLASS_CODE, 
	CLASSIFICATION_OF_BUSINESS, 
	EXTRACT_DATE AS EXTRACT_DATE, 
	AS_OF_DATE AS AS_OF_DATE, 
	RECORD_COUNT AS RECORD_COUNT, 
	SOURCE_SYSTEM_ID AS SOURCE_SYSTEM_ID, 
	AUDIT_ID AS AUDIT_ID
	FROM FIL_Inserts
),