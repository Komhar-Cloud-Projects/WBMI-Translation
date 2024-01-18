WITH
SQ_gtam_tm530xe_seq1_stage1 AS (
	SELECT
		gtam_tm530xe_seq1_stage_id,
		table_fld,
		key_len,
		location,
		policy_company,
		state,
		business_classification_code,
		seq_ind,
		future_use,
		expiration_date,
		data_len,
		long_desc,
		long_length,
		extract_date,
		as_of_date,
		record_count,
		source_system_id
	FROM gtam_tm530xe_seq1_stage1
),
LKP_arch_gtam_530xe_seq1_stage1 AS (
	SELECT
	arch_gtam_tm530xe_seq1_stage_id,
	long_length,
	long_desc,
	location,
	policy_company,
	state,
	business_classification_code,
	seq_ind,
	future_use,
	expiration_date
	FROM (
		SELECT tl.arch_gtam_tm530xe_seq1_stage_id as arch_gtam_tm530xe_seq1_stage_id
		      ,tl.gtam_tm530xe_seq1_stage_id as gtam_tm530xe_seq1_stage_id       
		      ,tl.location as location  
		      ,tl.policy_company as policy_company
		      ,tl.state as state
		      ,tl.business_classification_code as business_classification_code
		      ,tl.seq_ind as seq_ind
		      ,tl.future_use as future_use
		      ,tl.expiration_date as expiration_date 
		      ,tl.long_length as long_length 
		      ,tl.long_desc as long_desc      
		  FROM arch_gtam_tm530xe_seq1_stage tl
		   where 	tl.arch_gtam_tm530xe_seq1_stage_id In
			(Select max(arch_gtam_tm530xe_seq1_stage_id) from arch_gtam_tm530xe_seq1_stage b
			group by b.location
		      ,b.policy_company  
		      ,b.state 
		      ,b.business_classification_code  
		      ,b.seq_ind  
		      ,b.future_use  
		      ,b.expiration_date  )
		order by tl.location
		      ,tl.policy_company  
		      ,tl.state 
		      ,tl.business_classification_code  
		      ,tl.seq_ind  
		      ,tl.future_use  
		      ,tl.expiration_date --
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY location,policy_company,state,business_classification_code,seq_ind,future_use,expiration_date ORDER BY arch_gtam_tm530xe_seq1_stage_id) = 1
),
EXP_arch_tm530xe_seq1_stage AS (
	SELECT
	SQ_gtam_tm530xe_seq1_stage1.gtam_tm530xe_seq1_stage_id AS gtam_tm530xe_stage_id,
	SQ_gtam_tm530xe_seq1_stage1.table_fld AS Table_fld,
	SQ_gtam_tm530xe_seq1_stage1.key_len AS Key_len,
	SQ_gtam_tm530xe_seq1_stage1.location,
	SQ_gtam_tm530xe_seq1_stage1.policy_company,
	SQ_gtam_tm530xe_seq1_stage1.state,
	SQ_gtam_tm530xe_seq1_stage1.business_classification_code,
	SQ_gtam_tm530xe_seq1_stage1.seq_ind,
	SQ_gtam_tm530xe_seq1_stage1.future_use,
	SQ_gtam_tm530xe_seq1_stage1.expiration_date,
	SQ_gtam_tm530xe_seq1_stage1.data_len,
	SQ_gtam_tm530xe_seq1_stage1.long_desc,
	SQ_gtam_tm530xe_seq1_stage1.long_length,
	SQ_gtam_tm530xe_seq1_stage1.extract_date AS EXTRACT_DATE,
	SQ_gtam_tm530xe_seq1_stage1.as_of_date AS AS_OF_DATE,
	SQ_gtam_tm530xe_seq1_stage1.record_count AS RECORD_COUNT,
	SQ_gtam_tm530xe_seq1_stage1.source_system_id AS SOURCE_SYSTEM_ID,
	LKP_arch_gtam_530xe_seq1_stage1.arch_gtam_tm530xe_seq1_stage_id AS LKP_arch_gtam_tm530xe_seq1_stage_id,
	LKP_arch_gtam_530xe_seq1_stage1.long_length AS LKP_long_length,
	LKP_arch_gtam_530xe_seq1_stage1.long_desc AS LKP_long_desc,
	-- *INF*: iif(isnull(LKP_arch_gtam_tm530xe_seq1_stage_id),'NEW',
	--     iif(
	-- rtrim(ltrim( LKP_long_length )) <>  rtrim(ltrim( long_length ))    
	-- OR rtrim(ltrim( LKP_long_desc))    <> rtrim(ltrim( long_desc))     
	-- , 'UPDATE', 'NOCHANGE'))
	IFF(
	    LKP_arch_gtam_tm530xe_seq1_stage_id IS NULL, 'NEW',
	    IFF(
	        rtrim(ltrim(LKP_long_length)) <> rtrim(ltrim(long_length))
	        or rtrim(ltrim(LKP_long_desc)) <> rtrim(ltrim(long_desc)),
	        'UPDATE',
	        'NOCHANGE'
	    )
	) AS v_Changed_Flag,
	v_Changed_Flag AS Changed_Flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AUDIT_ID
	FROM SQ_gtam_tm530xe_seq1_stage1
	LEFT JOIN LKP_arch_gtam_530xe_seq1_stage1
	ON LKP_arch_gtam_530xe_seq1_stage1.location = SQ_gtam_tm530xe_seq1_stage1.location AND LKP_arch_gtam_530xe_seq1_stage1.policy_company = SQ_gtam_tm530xe_seq1_stage1.policy_company AND LKP_arch_gtam_530xe_seq1_stage1.state = SQ_gtam_tm530xe_seq1_stage1.state AND LKP_arch_gtam_530xe_seq1_stage1.business_classification_code = SQ_gtam_tm530xe_seq1_stage1.business_classification_code AND LKP_arch_gtam_530xe_seq1_stage1.seq_ind = SQ_gtam_tm530xe_seq1_stage1.seq_ind AND LKP_arch_gtam_530xe_seq1_stage1.future_use = SQ_gtam_tm530xe_seq1_stage1.future_use AND LKP_arch_gtam_530xe_seq1_stage1.expiration_date = SQ_gtam_tm530xe_seq1_stage1.expiration_date
),
FIL_Inserts AS (
	SELECT
	gtam_tm530xe_stage_id AS gtam_tm530xe_seq1_stage_id, 
	Table_fld, 
	Key_len, 
	location, 
	policy_company, 
	state, 
	business_classification_code, 
	seq_ind, 
	future_use, 
	expiration_date, 
	data_len, 
	long_desc, 
	long_length, 
	EXTRACT_DATE, 
	AS_OF_DATE, 
	RECORD_COUNT, 
	SOURCE_SYSTEM_ID, 
	Changed_Flag, 
	AUDIT_ID
	FROM EXP_arch_tm530xe_seq1_stage
	WHERE Changed_Flag = 'NEW' or Changed_Flag = 'UPDATE'
),
arch_gtam_tm530xe_seq1_stage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.arch_gtam_tm530xe_seq1_stage
	(gtam_tm530xe_seq1_stage_id, table_fld, key_len, location, policy_company, state, business_classification_code, seq_ind, future_use, expiration_date, data_len, long_desc, long_length, extract_date, as_of_date, record_count, source_system_id, audit_id)
	SELECT 
	GTAM_TM530XE_SEQ1_STAGE_ID, 
	Table_fld AS TABLE_FLD, 
	Key_len AS KEY_LEN, 
	LOCATION, 
	POLICY_COMPANY, 
	STATE, 
	BUSINESS_CLASSIFICATION_CODE, 
	SEQ_IND, 
	FUTURE_USE, 
	EXPIRATION_DATE, 
	DATA_LEN, 
	LONG_DESC, 
	LONG_LENGTH, 
	EXTRACT_DATE AS EXTRACT_DATE, 
	AS_OF_DATE AS AS_OF_DATE, 
	RECORD_COUNT AS RECORD_COUNT, 
	SOURCE_SYSTEM_ID AS SOURCE_SYSTEM_ID, 
	AUDIT_ID AS AUDIT_ID
	FROM FIL_Inserts
),