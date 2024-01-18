WITH
SQ_gtam_tl14_stage AS (
	SELECT
		gtam_tl14_stage_id,
		table_fld,
		key_len,
		location,
		master_company_number,
		reason_amended_code,
		language_indicator,
		data_len,
		alpha_descr_of_reason_amended,
		extract_date,
		as_of_date,
		record_count,
		source_system_id
	FROM gtam_tl14_stage1
),
LKP_arch_gtam_tl14_stage AS (
	SELECT
	arch_gtam_tl14_stage_id,
	alpha_descr_of_reason_amended,
	location,
	master_company_number,
	reason_amended_code,
	language_indicator
	FROM (
		SELECT 
		   tl.arch_gtam_tl14_stage_id as arch_gtam_tl14_stage_id  
		      , tl.location as location    
		      ,tl.master_company_number as master_company_number
		      ,tl.reason_amended_code as reason_amended_code
		       ,tl.language_indicator as language_indicator             
		      ,tl.alpha_descr_of_reason_amended as alpha_descr_of_reason_amended      
		  FROM arch_gtam_tl14_stage tl 
		  where 	tl.arch_gtam_tl14_stage_id In
			(Select max(arch_gtam_tl14_stage_id) from arch_gtam_tl14_stage b
			group by b.location ,b.master_company_number
		      ,b.reason_amended_code 
		      ,b.language_indicator )
		order by tl.location
		,tl.master_company_number
		      ,tl.reason_amended_code 
		      ,tl.language_indicator--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY location,master_company_number,reason_amended_code,language_indicator ORDER BY arch_gtam_tl14_stage_id DESC) = 1
),
EXP_arch_tl14_stage AS (
	SELECT
	SQ_gtam_tl14_stage.gtam_tl14_stage_id,
	SQ_gtam_tl14_stage.table_fld AS Table_fld,
	SQ_gtam_tl14_stage.key_len AS Key_len,
	SQ_gtam_tl14_stage.location,
	SQ_gtam_tl14_stage.master_company_number,
	SQ_gtam_tl14_stage.reason_amended_code,
	SQ_gtam_tl14_stage.language_indicator,
	SQ_gtam_tl14_stage.data_len,
	SQ_gtam_tl14_stage.alpha_descr_of_reason_amended,
	SQ_gtam_tl14_stage.extract_date AS EXTRACT_DATE,
	SQ_gtam_tl14_stage.as_of_date AS AS_OF_DATE,
	SQ_gtam_tl14_stage.record_count AS RECORD_COUNT,
	SQ_gtam_tl14_stage.source_system_id AS SOURCE_SYSTEM_ID,
	LKP_arch_gtam_tl14_stage.arch_gtam_tl14_stage_id AS LKP_arch_gtam_tl07rx_stage_id,
	LKP_arch_gtam_tl14_stage.alpha_descr_of_reason_amended AS LKP_alpha_descr_of_reason_amended,
	-- *INF*: iif(isnull(LKP_arch_gtam_tl07rx_stage_id),'NEW',
	--     iif((  ltrim(rtrim(LKP_alpha_descr_of_reason_amended)) <>  ltrim(rtrim(alpha_descr_of_reason_amended))
	-- ), 'UPDATE', 'NOCHANGE'))
	IFF(
	    LKP_arch_gtam_tl07rx_stage_id IS NULL, 'NEW',
	    IFF(
	        (ltrim(rtrim(LKP_alpha_descr_of_reason_amended)) <> ltrim(rtrim(alpha_descr_of_reason_amended))),
	        'UPDATE',
	        'NOCHANGE'
	    )
	) AS v_Changed_Flag,
	v_Changed_Flag AS Changed_Flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AUDIT_ID
	FROM SQ_gtam_tl14_stage
	LEFT JOIN LKP_arch_gtam_tl14_stage
	ON LKP_arch_gtam_tl14_stage.location = SQ_gtam_tl14_stage.location AND LKP_arch_gtam_tl14_stage.master_company_number = SQ_gtam_tl14_stage.master_company_number AND LKP_arch_gtam_tl14_stage.reason_amended_code = SQ_gtam_tl14_stage.reason_amended_code AND LKP_arch_gtam_tl14_stage.language_indicator = SQ_gtam_tl14_stage.language_indicator
),
FIL_Inserts1 AS (
	SELECT
	gtam_tl14_stage_id, 
	Table_fld, 
	Key_len, 
	location, 
	master_company_number, 
	reason_amended_code, 
	language_indicator, 
	data_len, 
	alpha_descr_of_reason_amended, 
	EXTRACT_DATE, 
	AS_OF_DATE, 
	RECORD_COUNT, 
	SOURCE_SYSTEM_ID, 
	Changed_Flag, 
	AUDIT_ID
	FROM EXP_arch_tl14_stage
	WHERE Changed_Flag = 'NEW' or Changed_Flag = 'UPDATE'
),
arch_gtam_tl14_stage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.arch_gtam_tl14_stage
	(gtam_tl14_stage_id, table_fld, key_len, location, master_company_number, reason_amended_code, language_indicator, data_len, alpha_descr_of_reason_amended, extract_date, as_of_date, record_count, source_system_id, audit_id)
	SELECT 
	GTAM_TL14_STAGE_ID, 
	Table_fld AS TABLE_FLD, 
	Key_len AS KEY_LEN, 
	LOCATION, 
	MASTER_COMPANY_NUMBER, 
	REASON_AMENDED_CODE, 
	LANGUAGE_INDICATOR, 
	DATA_LEN, 
	ALPHA_DESCR_OF_REASON_AMENDED, 
	EXTRACT_DATE AS EXTRACT_DATE, 
	AS_OF_DATE AS AS_OF_DATE, 
	RECORD_COUNT AS RECORD_COUNT, 
	SOURCE_SYSTEM_ID AS SOURCE_SYSTEM_ID, 
	AUDIT_ID AS AUDIT_ID
	FROM FIL_Inserts1
),