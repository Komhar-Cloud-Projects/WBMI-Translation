WITH
SQ_gtam_wbrsnca_stage1 AS (
	SELECT
		gtam_wbrsnca_stage_id,
		table_fld,
		key_len,
		cancellation_reason_code,
		data_len,
		cancellation_reason_descript,
		extract_date,
		as_of_date,
		record_count,
		source_system_id
	FROM gtam_wbrsnca_stage1
),
LKP_ARCH_GTAM_WBRSNCA_STAGE AS (
	SELECT
	arch_gtam_wbrsnca_stage_id,
	cancellation_reason_descript,
	cancellation_reason_code
	FROM (
		SELECT  tl.arch_gtam_wbrsnca_stage_id   as arch_gtam_wbrsnca_stage_id      
		            , tl.cancellation_reason_code     as cancellation_reason_code
		            , tl.cancellation_reason_descript as cancellation_reason_descript    
		  FROM arch_gtam_wbrsnca_stage tl
		  where tl.arch_gtam_wbrsnca_stage_id  IN
		  (Select max(arch_gtam_wbrsnca_stage_id ) from arch_gtam_wbrsnca_stage b
			group by b.cancellation_reason_code )
			order by tl.cancellation_reason_code--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY cancellation_reason_code ORDER BY arch_gtam_wbrsnca_stage_id DESC) = 1
),
EXP_arch_wbrsnca_stage AS (
	SELECT
	SQ_gtam_wbrsnca_stage1.gtam_wbrsnca_stage_id,
	SQ_gtam_wbrsnca_stage1.table_fld AS Table_fld,
	SQ_gtam_wbrsnca_stage1.key_len AS Key_len,
	SQ_gtam_wbrsnca_stage1.cancellation_reason_code,
	SQ_gtam_wbrsnca_stage1.data_len,
	SQ_gtam_wbrsnca_stage1.cancellation_reason_descript,
	SQ_gtam_wbrsnca_stage1.extract_date AS EXTRACT_DATE,
	SQ_gtam_wbrsnca_stage1.as_of_date AS AS_OF_DATE,
	SQ_gtam_wbrsnca_stage1.record_count AS RECORD_COUNT,
	SQ_gtam_wbrsnca_stage1.source_system_id AS SOURCE_SYSTEM_ID,
	LKP_ARCH_GTAM_WBRSNCA_STAGE.arch_gtam_wbrsnca_stage_id AS LKP_arch_gtam_wbrsnca_stage_id,
	LKP_ARCH_GTAM_WBRSNCA_STAGE.cancellation_reason_descript AS LKP_cancellation_reason_descript,
	-- *INF*: iif(isnull(LKP_arch_gtam_wbrsnca_stage_id),'NEW',
	--     iif((  ltrim(rtrim(LKP_cancellation_reason_descript))<>  ltrim(rtrim(cancellation_reason_descript))
	-- ), 'UPDATE', 'NOCHANGE'))
	IFF(
	    LKP_arch_gtam_wbrsnca_stage_id IS NULL, 'NEW',
	    IFF(
	        (ltrim(rtrim(LKP_cancellation_reason_descript)) <> ltrim(rtrim(cancellation_reason_descript))),
	        'UPDATE',
	        'NOCHANGE'
	    )
	) AS v_Changed_Flag,
	v_Changed_Flag AS Changed_Flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AUDIT_ID
	FROM SQ_gtam_wbrsnca_stage1
	LEFT JOIN LKP_ARCH_GTAM_WBRSNCA_STAGE
	ON LKP_ARCH_GTAM_WBRSNCA_STAGE.cancellation_reason_code = SQ_gtam_wbrsnca_stage1.cancellation_reason_code
),
FIL_Inserts AS (
	SELECT
	gtam_wbrsnca_stage_id, 
	Table_fld, 
	Key_len, 
	cancellation_reason_code, 
	data_len, 
	cancellation_reason_descript, 
	EXTRACT_DATE, 
	AS_OF_DATE, 
	RECORD_COUNT, 
	SOURCE_SYSTEM_ID, 
	Changed_Flag, 
	AUDIT_ID
	FROM EXP_arch_wbrsnca_stage
	WHERE Changed_Flag = 'NEW' or Changed_Flag = 'UPDATE'
),
arch_gtam_wbrsnca_stage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.arch_gtam_wbrsnca_stage
	(gtam_wbrsnca_stage_id, table_fld, key_len, cancellation_reason_code, data_len, cancellation_reason_descript, extract_date, as_of_date, record_count, source_system_id, audit_id)
	SELECT 
	GTAM_WBRSNCA_STAGE_ID, 
	Table_fld AS TABLE_FLD, 
	Key_len AS KEY_LEN, 
	CANCELLATION_REASON_CODE, 
	DATA_LEN, 
	CANCELLATION_REASON_DESCRIPT, 
	EXTRACT_DATE AS EXTRACT_DATE, 
	AS_OF_DATE AS AS_OF_DATE, 
	RECORD_COUNT AS RECORD_COUNT, 
	SOURCE_SYSTEM_ID AS SOURCE_SYSTEM_ID, 
	AUDIT_ID AS AUDIT_ID
	FROM FIL_Inserts
),