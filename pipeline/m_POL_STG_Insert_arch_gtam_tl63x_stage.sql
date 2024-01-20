WITH
SQ_gtam_tl63x_stage1 AS (
	SELECT
		gtam_tl63x_stage_id,
		table_fld,
		key_len,
		location,
		master_company_number,
		location_state,
		class_description_code,
		crime_ind,
		eff_date,
		class_desc_num_seq,
		data_len,
		one_line_of_class_desc,
		reg_class_desc_code,
		extract_date,
		as_of_date,
		record_count,
		source_system_id
	FROM gtam_tl63x_stage1
),
LKP_arch_gtam_tl63x_stage AS (
	SELECT
	arch_gtam_tl63x_stage_id,
	one_line_of_class_desc,
	reg_class_desc_code,
	location,
	master_company_number,
	location_state,
	class_description_code,
	crime_ind,
	eff_date,
	class_desc_num_seq
	FROM (
		SELECT tl.arch_gtam_tl63x_stage_id as arch_gtam_tl63x_stage_id          
		      ,tl.location as location
		      ,tl.master_company_number as master_company_number
		      ,tl.location_state as location_state 
		      ,tl.class_description_code as class_description_code
		      ,tl.crime_ind as crime_ind 
		      ,tl.eff_date as eff_date
		      ,tl.class_desc_num_seq as   class_desc_num_seq     
		      ,tl.one_line_of_class_desc as one_line_of_class_desc
		      ,tl.reg_class_desc_code as reg_class_desc_code       
		  FROM  arch_gtam_tl63x_stage tl 
		   where 	tl.arch_gtam_tl63x_stage_id  In
			(Select max(arch_gtam_tl63x_stage_id ) from arch_gtam_tl63x_stage b
			group by b.location, b.master_company_number  
		      ,b.location_state 
		      ,b.class_description_code  
		      ,b.crime_ind  
		      ,b.eff_date  
		      ,b.class_desc_num_seq)
		order by tl.location,tl.master_company_number  
		      ,tl.location_state 
		      ,tl.class_description_code  
		      ,tl.crime_ind  
		      ,tl.eff_date  
		      ,tl.class_desc_num_seq--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY location,master_company_number,location_state,class_description_code,crime_ind,eff_date,class_desc_num_seq ORDER BY arch_gtam_tl63x_stage_id) = 1
),
EXP_arch_tl63x_stage AS (
	SELECT
	SQ_gtam_tl63x_stage1.gtam_tl63x_stage_id,
	SQ_gtam_tl63x_stage1.table_fld AS Table_fld,
	SQ_gtam_tl63x_stage1.key_len AS Key_len,
	SQ_gtam_tl63x_stage1.location,
	SQ_gtam_tl63x_stage1.master_company_number,
	SQ_gtam_tl63x_stage1.location_state,
	SQ_gtam_tl63x_stage1.class_description_code,
	SQ_gtam_tl63x_stage1.crime_ind,
	SQ_gtam_tl63x_stage1.eff_date,
	SQ_gtam_tl63x_stage1.class_desc_num_seq,
	SQ_gtam_tl63x_stage1.data_len,
	SQ_gtam_tl63x_stage1.one_line_of_class_desc,
	SQ_gtam_tl63x_stage1.reg_class_desc_code,
	SQ_gtam_tl63x_stage1.extract_date AS EXTRACT_DATE,
	SQ_gtam_tl63x_stage1.as_of_date AS AS_OF_DATE,
	SQ_gtam_tl63x_stage1.record_count AS RECORD_COUNT,
	SQ_gtam_tl63x_stage1.source_system_id AS SOURCE_SYSTEM_ID,
	LKP_arch_gtam_tl63x_stage.arch_gtam_tl63x_stage_id AS LKP_arch_gtam_tl63x_stage_id,
	LKP_arch_gtam_tl63x_stage.one_line_of_class_desc AS LKP_one_line_of_class_desc,
	LKP_arch_gtam_tl63x_stage.reg_class_desc_code AS LKP_reg_class_desc_code,
	-- *INF*: iif(isnull(LKP_arch_gtam_tl63x_stage_id),'NEW',
	--     iif( ltrim(rtrim(LKP_one_line_of_class_desc))<>  ltrim(rtrim(one_line_of_class_desc))
	--           OR  ltrim(rtrim(LKP_reg_class_desc_code)) <> ltrim(rtrim(reg_class_desc_code))
	-- , 'UPDATE', 'NOCHANGE'))
	IFF(
	    LKP_arch_gtam_tl63x_stage_id IS NULL, 'NEW',
	    IFF(
	        ltrim(rtrim(LKP_one_line_of_class_desc)) <> ltrim(rtrim(one_line_of_class_desc))
	        or ltrim(rtrim(LKP_reg_class_desc_code)) <> ltrim(rtrim(reg_class_desc_code)),
	        'UPDATE',
	        'NOCHANGE'
	    )
	) AS v_Changed_Flag,
	v_Changed_Flag AS Changed_Flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AUDIT_ID
	FROM SQ_gtam_tl63x_stage1
	LEFT JOIN LKP_arch_gtam_tl63x_stage
	ON LKP_arch_gtam_tl63x_stage.location = SQ_gtam_tl63x_stage1.location AND LKP_arch_gtam_tl63x_stage.master_company_number = SQ_gtam_tl63x_stage1.master_company_number AND LKP_arch_gtam_tl63x_stage.location_state = SQ_gtam_tl63x_stage1.location_state AND LKP_arch_gtam_tl63x_stage.class_description_code = SQ_gtam_tl63x_stage1.class_description_code AND LKP_arch_gtam_tl63x_stage.crime_ind = SQ_gtam_tl63x_stage1.crime_ind AND LKP_arch_gtam_tl63x_stage.eff_date = SQ_gtam_tl63x_stage1.eff_date AND LKP_arch_gtam_tl63x_stage.class_desc_num_seq = SQ_gtam_tl63x_stage1.class_desc_num_seq
),
FIL_Inserts AS (
	SELECT
	gtam_tl63x_stage_id, 
	Table_fld, 
	Key_len, 
	location, 
	master_company_number, 
	location_state, 
	class_description_code, 
	crime_ind, 
	eff_date, 
	class_desc_num_seq, 
	data_len, 
	one_line_of_class_desc, 
	reg_class_desc_code, 
	EXTRACT_DATE, 
	AS_OF_DATE, 
	RECORD_COUNT, 
	SOURCE_SYSTEM_ID, 
	Changed_Flag, 
	AUDIT_ID
	FROM EXP_arch_tl63x_stage
	WHERE Changed_Flag = 'NEW' or Changed_Flag = 'UPDATE'
),
arch_gtam_tl63x_stage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.arch_gtam_tl63x_stage
	(gtam_tl63x_stage_id, table_fld, key_len, location, master_company_number, location_state, class_description_code, crime_ind, eff_date, class_desc_num_seq, data_len, one_line_of_class_desc, reg_class_desc_code, extract_date, as_of_date, record_count, source_system_id, audit_id)
	SELECT 
	GTAM_TL63X_STAGE_ID, 
	Table_fld AS TABLE_FLD, 
	Key_len AS KEY_LEN, 
	LOCATION, 
	MASTER_COMPANY_NUMBER, 
	LOCATION_STATE, 
	CLASS_DESCRIPTION_CODE, 
	CRIME_IND, 
	EFF_DATE, 
	CLASS_DESC_NUM_SEQ, 
	DATA_LEN, 
	ONE_LINE_OF_CLASS_DESC, 
	REG_CLASS_DESC_CODE, 
	EXTRACT_DATE AS EXTRACT_DATE, 
	AS_OF_DATE AS AS_OF_DATE, 
	RECORD_COUNT AS RECORD_COUNT, 
	SOURCE_SYSTEM_ID AS SOURCE_SYSTEM_ID, 
	AUDIT_ID AS AUDIT_ID
	FROM FIL_Inserts
),