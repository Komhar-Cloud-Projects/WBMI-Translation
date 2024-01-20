WITH
SQ_gtam_wbadj_stage AS (
	SELECT
		gtam_wbadj_stage_id,
		table_fld,
		key_len,
		Adjuster_Code,
		data_len,
		Adjuster_Initial_Code,
		Adjuster_Name,
		Cost_Center_Number,
		Telephone_Number,
		Extension_Number,
		Fax_Number,
		Email_Address,
		extract_date,
		as_of_date,
		record_count,
		source_system_id
	FROM gtam_wbadj_stage
),
EXP_arch_GTAM_wbadj_stage AS (
	SELECT
	gtam_wbadj_stage_id AS wbadj_stage_ID,
	table_fld AS TABLE_FLD,
	key_len AS KEY_LEN,
	Adjuster_Code AS ADJUSTER_CODE,
	data_len AS DATA_LEN,
	Adjuster_Initial_Code,
	Adjuster_Name,
	Cost_Center_Number,
	Telephone_Number,
	Extension_Number,
	Fax_Number,
	Email_Address,
	extract_date AS EXTRACT_DATE,
	as_of_date AS AS_OF_DATE,
	record_count AS RECORD_COUNT,
	source_system_id AS SOURCE_SYSTEM_ID,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AUDIT_ID_OP
	FROM SQ_gtam_wbadj_stage
),
arch_gtam_wbadj_stage AS (
	INSERT INTO arch_gtam_wbadj_stage
	(gtam_wbadj_stage_id, table_fld, key_len, Adjuster_Code, data_len, Adjuster_Initial_Code, Adjuster_Name, Cost_Center_Number, Telephone_Number, Extension_Number, Fax_Number, Email_Address, extract_date, as_of_date, record_count, source_system_id, audit_id)
	SELECT 
	wbadj_stage_ID AS GTAM_WBADJ_STAGE_ID, 
	TABLE_FLD AS TABLE_FLD, 
	KEY_LEN AS KEY_LEN, 
	ADJUSTER_CODE AS ADJUSTER_CODE, 
	DATA_LEN AS DATA_LEN, 
	ADJUSTER_INITIAL_CODE, 
	ADJUSTER_NAME, 
	COST_CENTER_NUMBER, 
	TELEPHONE_NUMBER, 
	EXTENSION_NUMBER, 
	FAX_NUMBER, 
	EMAIL_ADDRESS, 
	EXTRACT_DATE AS EXTRACT_DATE, 
	AS_OF_DATE AS AS_OF_DATE, 
	RECORD_COUNT AS RECORD_COUNT, 
	SOURCE_SYSTEM_ID AS SOURCE_SYSTEM_ID, 
	AUDIT_ID_OP AS AUDIT_ID
	FROM EXP_arch_GTAM_wbadj_stage
),