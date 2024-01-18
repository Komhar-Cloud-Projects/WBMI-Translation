WITH
SQ_gtam_wbimcls_stage AS (
	SELECT
		gtam_wbimcls_stage_id,
		table_fld,
		key_len,
		inland_marine_class_code,
		data_len,
		inland_marine_class_description,
		extract_date,
		as_of_date,
		record_count,
		source_system_id
	FROM gtam_wbimcls_stage
),
LKP_arch_gtam_wbimcls_stage AS (
	SELECT
	arch_gtam_wbimcls_stage_id,
	inland_marine_class_description,
	inland_marine_class_code
	FROM (
		SELECT tl.arch_gtam_wbimcls_stage_id as arch_gtam_wbimcls_stage_id          
		      ,tl.inland_marine_class_code as  inland_marine_class_code 
		      ,tl.inland_marine_class_description as inland_marine_class_description       
		  FROM  arch_gtam_wbimcls_stage tl 
		   where 	tl.arch_gtam_wbimcls_stage_id  In
			(Select max(arch_gtam_wbimcls_stage_id ) from arch_gtam_wbimcls_stage b
			group by b.inland_marine_class_code)
		order by tl.inland_marine_class_code--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY inland_marine_class_code ORDER BY arch_gtam_wbimcls_stage_id) = 1
),
EXP_arch_wbimcls_stage AS (
	SELECT
	SQ_gtam_wbimcls_stage.gtam_wbimcls_stage_id,
	SQ_gtam_wbimcls_stage.table_fld AS Table_fld,
	SQ_gtam_wbimcls_stage.key_len AS Key_len,
	SQ_gtam_wbimcls_stage.inland_marine_class_code,
	SQ_gtam_wbimcls_stage.data_len,
	SQ_gtam_wbimcls_stage.inland_marine_class_description,
	SQ_gtam_wbimcls_stage.extract_date AS EXTRACT_DATE,
	SQ_gtam_wbimcls_stage.as_of_date AS AS_OF_DATE,
	SQ_gtam_wbimcls_stage.record_count AS RECORD_COUNT,
	SQ_gtam_wbimcls_stage.source_system_id AS SOURCE_SYSTEM_ID,
	LKP_arch_gtam_wbimcls_stage.arch_gtam_wbimcls_stage_id AS LKP_arch_gtam_wbimcls_stage_id,
	LKP_arch_gtam_wbimcls_stage.inland_marine_class_description AS LKP_inland_marine_class_description,
	-- *INF*: iif(isnull(LKP_arch_gtam_wbimcls_stage_id),'NEW',
	--     iif(  
	-- ltrim(rtrim(LKP_inland_marine_class_description))
	-- <>  ltrim(rtrim(inland_marine_class_description))
	-- , 'UPDATE', 'NOCHANGE'))
	IFF(
	    LKP_arch_gtam_wbimcls_stage_id IS NULL, 'NEW',
	    IFF(
	        ltrim(rtrim(LKP_inland_marine_class_description)) <> ltrim(rtrim(inland_marine_class_description)),
	        'UPDATE',
	        'NOCHANGE'
	    )
	) AS v_Changed_Flag,
	v_Changed_Flag AS Changed_Flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AUDIT_ID
	FROM SQ_gtam_wbimcls_stage
	LEFT JOIN LKP_arch_gtam_wbimcls_stage
	ON LKP_arch_gtam_wbimcls_stage.inland_marine_class_code = SQ_gtam_wbimcls_stage.inland_marine_class_code
),
FIL_Inserts AS (
	SELECT
	gtam_wbimcls_stage_id AS gtam_wbmicls_stage_id, 
	Table_fld, 
	Key_len, 
	inland_marine_class_code, 
	data_len, 
	inland_marine_class_description, 
	EXTRACT_DATE, 
	AS_OF_DATE, 
	RECORD_COUNT, 
	SOURCE_SYSTEM_ID, 
	Changed_Flag, 
	AUDIT_ID
	FROM EXP_arch_wbimcls_stage
	WHERE Changed_Flag = 'NEW' or Changed_Flag = 'UPDATE'
),
arch_gtam_wbimcls_stage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.arch_gtam_wbimcls_stage
	(gtam_wbimcls_stage_id, table_fld, key_len, inland_marine_class_code, data_len, inland_marine_class_description, extract_date, as_of_date, record_count, source_system_id, audit_id)
	SELECT 
	gtam_wbmicls_stage_id AS GTAM_WBIMCLS_STAGE_ID, 
	Table_fld AS TABLE_FLD, 
	Key_len AS KEY_LEN, 
	INLAND_MARINE_CLASS_CODE, 
	DATA_LEN, 
	INLAND_MARINE_CLASS_DESCRIPTION, 
	EXTRACT_DATE AS EXTRACT_DATE, 
	AS_OF_DATE AS AS_OF_DATE, 
	RECORD_COUNT AS RECORD_COUNT, 
	SOURCE_SYSTEM_ID AS SOURCE_SYSTEM_ID, 
	AUDIT_ID AS AUDIT_ID
	FROM FIL_Inserts
),