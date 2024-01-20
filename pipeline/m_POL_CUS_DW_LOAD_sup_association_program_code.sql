WITH
SQ_gtam_wbclb_stage AS (
	SELECT     
		b.sup_assoc_prog_type_id  as gtam_wbclb_stage_id , 
		a.prog_code,
		a.prog_description,
		b.assoc_prog_type_descript as prog_type
	FROM         
		@{pipeline().parameters.SOURCE_TABLE_OWNER}.gtam_wbclb_stage a
	LEFT OUTER JOIN  
		@{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.sup_association_program_type b
	ON
		a.prog_code = b.assoc_prog_type_code
	ORDER BY 
		a.prog_code
	
	
	
	
	--- Here we are performing left outer join between 
	--gtam_wbclb_stage &  rpt_edm.dbo.sup_assoc_program_type 
	-- tables  on prog_code = assoc_program_type_code
),
EXP_values AS (
	SELECT
	gtam_wbclb_stage_id AS in_sup_assoc_program_type_id,
	-- *INF*: iif(isnull(in_sup_assoc_program_type_id),1,0)
	-- 
	-- 
	-- -- these are missing values which must be output to a flat file and sent to Jody by email.
	-- -- this will be inplemented in later iterations
	IFF(in_sup_assoc_program_type_id IS NULL, 1, 0) AS sup_assoc_program_type_id,
	prog_code AS in_prog_code,
	-- *INF*: iif(isnull(in_prog_code) or IS_SPACES(in_prog_code) or LENGTH(in_prog_code)=0,'N/A',LTRIM(RTRIM(in_prog_code)))
	IFF(
	    in_prog_code IS NULL
	    or LENGTH(in_prog_code)>0
	    and TRIM(in_prog_code)=''
	    or LENGTH(in_prog_code) = 0,
	    'N/A',
	    LTRIM(RTRIM(in_prog_code))
	) AS assoc_prog_code,
	prog_description AS in_prog_description,
	-- *INF*: iif(isnull(in_prog_description) or IS_SPACES(in_prog_description) or LENGTH(in_prog_description)=0,'N/A',LTRIM(RTRIM(in_prog_description)))
	IFF(
	    in_prog_description IS NULL
	    or LENGTH(in_prog_description)>0
	    and TRIM(in_prog_description)=''
	    or LENGTH(in_prog_description) = 0,
	    'N/A',
	    LTRIM(RTRIM(in_prog_description))
	) AS assoc_prog_code_descript,
	prog_type AS in_assoc_program_type,
	-- *INF*: iif(isnull(in_assoc_program_type) or IS_SPACES(in_assoc_program_type) or LENGTH(in_assoc_program_type)=0,'N/A',LTRIM(RTRIM(in_assoc_program_type)))
	IFF(
	    in_assoc_program_type IS NULL
	    or LENGTH(in_assoc_program_type)>0
	    and TRIM(in_assoc_program_type)=''
	    or LENGTH(in_assoc_program_type) = 0,
	    'N/A',
	    LTRIM(RTRIM(in_assoc_program_type))
	) AS assoc_prog_type
	FROM SQ_gtam_wbclb_stage
),
LKP_sup_association_program_code AS (
	SELECT
	sup_assoc_prog_code_id,
	assoc_prog_code_descript,
	assoc_prog_type,
	assoc_prog_code
	FROM (
		SELECT 
		sup_association_program_code.sup_assoc_prog_code_id as sup_assoc_prog_code_id, sup_association_program_code.assoc_prog_code_descript as assoc_prog_code_descript, sup_association_program_code.assoc_prog_type as assoc_prog_type,
		LTRIM(RTRIM(sup_association_program_code.assoc_prog_code)) as assoc_prog_code 
		FROM 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.sup_association_program_code
		WHERE
		sup_association_program_code.crrnt_snpsht_flag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY assoc_prog_code ORDER BY sup_assoc_prog_code_id DESC) = 1
),
EXP_Detect_Changes AS (
	SELECT
	LKP_sup_association_program_code.sup_assoc_prog_code_id AS lkp_sup_assoc_program_code_id,
	LKP_sup_association_program_code.assoc_prog_code_descript AS lkp_prog_description,
	LKP_sup_association_program_code.assoc_prog_type AS lkp_assoc_program_type,
	EXP_values.assoc_prog_code,
	EXP_values.assoc_prog_code_descript,
	EXP_values.assoc_prog_type,
	-- *INF*: iif(isnull(lkp_sup_assoc_program_code_id),'NEW',IIF(
	-- LTRIM(RTRIM(lkp_prog_description)) != LTRIM(RTRIM(assoc_prog_code_descript)) OR
	-- LTRIM(RTRIM(lkp_assoc_program_type)) != LTRIM(RTRIM(assoc_prog_type)),'UPDATE','NOCHANGE'))
	-- 
	-- 
	-- 
	-- 
	-- 
	-- 
	-- 
	-- --iif(isnull(lkp_sup_assoc_program_code_id),'NEW',IIF(
	-- --LTRIM(RTRIM(lkp_prog_description)) != LTRIM(RTRIM(prog_description)) ,'UPDATE','NOCHANGE'))
	IFF(
	    lkp_sup_assoc_program_code_id IS NULL, 'NEW',
	    IFF(
	        LTRIM(RTRIM(lkp_prog_description)) != LTRIM(RTRIM(assoc_prog_code_descript))
	        or LTRIM(RTRIM(lkp_assoc_program_type)) != LTRIM(RTRIM(assoc_prog_type)),
	        'UPDATE',
	        'NOCHANGE'
	    )
	) AS v_changed_flag,
	v_changed_flag AS changed_flag,
	1 AS crrnt_snpsht_flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	-- *INF*: iif(v_changed_flag='NEW',
	-- 	to_date('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'),sysdate)
	IFF(
	    v_changed_flag = 'NEW', TO_TIMESTAMP('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'),
	    CURRENT_TIMESTAMP
	) AS eff_from_date,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_TIMESTAMP('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS eff_to_date,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS source_sys_id,
	SYSDATE AS created_date,
	SYSDATE AS modified_date
	FROM EXP_values
	LEFT JOIN LKP_sup_association_program_code
	ON LKP_sup_association_program_code.assoc_prog_code = EXP_values.assoc_prog_code
),
FIL_insert AS (
	SELECT
	assoc_prog_code, 
	assoc_prog_code_descript, 
	assoc_prog_type, 
	changed_flag, 
	crrnt_snpsht_flag, 
	audit_id, 
	eff_from_date, 
	eff_to_date, 
	source_sys_id, 
	created_date, 
	modified_date
	FROM EXP_Detect_Changes
	WHERE changed_flag='NEW' OR changed_flag='UPDATE'
),
TGT_sup_association_program_code_INSERT AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_association_program_code
	(crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date, assoc_prog_code, assoc_prog_code_descript, assoc_prog_type)
	SELECT 
	CRRNT_SNPSHT_FLAG, 
	AUDIT_ID, 
	EFF_FROM_DATE, 
	EFF_TO_DATE, 
	SOURCE_SYS_ID, 
	CREATED_DATE, 
	MODIFIED_DATE, 
	ASSOC_PROG_CODE, 
	ASSOC_PROG_CODE_DESCRIPT, 
	ASSOC_PROG_TYPE
	FROM FIL_insert
),
SQ_gtam_wbprg_stage AS (
	SELECT
		gtam_wbprg_stage_id,
		prog_id,
		exp_date,
		prog_name,
		extract_date,
		as_of_date,
		rcrd_count,
		source_sys_id
	FROM gtam_wbprg_stage
),
EXP_wbprg_values AS (
	SELECT
	gtam_wbprg_stage_id,
	prog_id,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(prog_id)
	UDF_DEFAULT_VALUE_FOR_STRINGS(prog_id) AS prog_code,
	prog_name,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(prog_name)
	UDF_DEFAULT_VALUE_FOR_STRINGS(prog_name) AS prog_code_description
	FROM SQ_gtam_wbprg_stage
),
LKP_sup_association_program_code1 AS (
	SELECT
	sup_assoc_prog_code_id,
	assoc_prog_code_descript,
	assoc_prog_type,
	assoc_prog_code
	FROM (
		SELECT 
		sup_association_program_code.sup_assoc_prog_code_id as sup_assoc_prog_code_id, sup_association_program_code.assoc_prog_code_descript as assoc_prog_code_descript, sup_association_program_code.assoc_prog_type as assoc_prog_type,
		LTRIM(RTRIM(sup_association_program_code.assoc_prog_code)) as assoc_prog_code 
		FROM 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.sup_association_program_code
		WHERE
		sup_association_program_code.crrnt_snpsht_flag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY assoc_prog_code ORDER BY sup_assoc_prog_code_id DESC) = 1
),
EXP_Detect_Changes1 AS (
	SELECT
	LKP_sup_association_program_code1.sup_assoc_prog_code_id AS lkp_sup_assoc_program_code_id,
	LKP_sup_association_program_code1.assoc_prog_code_descript AS lkp_prog_description,
	LKP_sup_association_program_code1.assoc_prog_type AS lkp_assoc_program_type,
	EXP_wbprg_values.prog_code AS assoc_prog_code,
	EXP_wbprg_values.prog_code_description AS assoc_prog_code_descript,
	'Program' AS v_assoc_prog_type,
	v_assoc_prog_type AS assoc_prog_type_out,
	-- *INF*: iif(isnull(lkp_sup_assoc_program_code_id),'NEW',IIF(
	-- LTRIM(RTRIM(lkp_prog_description)) != LTRIM(RTRIM(assoc_prog_code_descript)) OR
	-- LTRIM(RTRIM(lkp_assoc_program_type)) !=v_assoc_prog_type,'UPDATE','NOCHANGE'))
	-- 
	-- 
	-- 
	-- 
	-- 
	-- 
	-- 
	-- --iif(isnull(lkp_sup_assoc_program_code_id),'NEW',IIF(
	-- --LTRIM(RTRIM(lkp_prog_description)) != LTRIM(RTRIM(prog_description)) ,'UPDATE','NOCHANGE'))
	IFF(
	    lkp_sup_assoc_program_code_id IS NULL, 'NEW',
	    IFF(
	        LTRIM(RTRIM(lkp_prog_description)) != LTRIM(RTRIM(assoc_prog_code_descript))
	        or LTRIM(RTRIM(lkp_assoc_program_type)) != v_assoc_prog_type,
	        'UPDATE',
	        'NOCHANGE'
	    )
	) AS v_changed_flag,
	v_changed_flag AS changed_flag,
	1 AS crrnt_snpsht_flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	-- *INF*: iif(v_changed_flag='NEW',
	-- 	to_date('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'),sysdate)
	IFF(
	    v_changed_flag = 'NEW', TO_TIMESTAMP('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'),
	    CURRENT_TIMESTAMP
	) AS eff_from_date,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_TIMESTAMP('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS eff_to_date,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS source_sys_id,
	SYSDATE AS created_date,
	SYSDATE AS modified_date
	FROM EXP_wbprg_values
	LEFT JOIN LKP_sup_association_program_code1
	ON LKP_sup_association_program_code1.assoc_prog_code = EXP_wbprg_values.prog_code
),
FIL_insert1 AS (
	SELECT
	assoc_prog_code, 
	assoc_prog_code_descript, 
	assoc_prog_type_out AS assoc_prog_type, 
	changed_flag, 
	crrnt_snpsht_flag, 
	audit_id, 
	eff_from_date, 
	eff_to_date, 
	source_sys_id, 
	created_date, 
	modified_date
	FROM EXP_Detect_Changes1
	WHERE changed_flag='NEW' OR changed_flag='UPDATE'
),
TGT_sup_association_program_code_INSERT1 AS (
	INSERT INTO sup_association_program_code
	(crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date, assoc_prog_code, assoc_prog_code_descript, assoc_prog_type)
	SELECT 
	CRRNT_SNPSHT_FLAG, 
	AUDIT_ID, 
	EFF_FROM_DATE, 
	EFF_TO_DATE, 
	SOURCE_SYS_ID, 
	CREATED_DATE, 
	MODIFIED_DATE, 
	ASSOC_PROG_CODE, 
	ASSOC_PROG_CODE_DESCRIPT, 
	ASSOC_PROG_TYPE
	FROM FIL_insert1
),
SQ_sup_association_program_code AS (
	SELECT 
			a.sup_assoc_prog_code_id, 
			a.eff_from_date, 
			a.eff_to_date, 
			a.assoc_prog_code 
	FROM
			 @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_association_program_code a
	WHERE 
			a.assoc_prog_code  IN 
			(SELECT assoc_prog_code FROM sup_association_program_code 
			WHERE crrnt_snpsht_flag = 1 GROUP BY assoc_prog_code  HAVING count(*) > 1)
	ORDER BY 
			a.assoc_prog_code , a.eff_from_date  DESC
	
	
	--IN Subquery exists to pick AK ID column values that have multiple rows with a 12/31/2100 eff_to_date.
	--When this condition occurs this is an indication that we must expire one or more of these rows.
	--WHERE clause is always made up of current snapshot flag 
	--GROUP BY clause is always on AK
	--HAVING clause stays the same
),
EXP_Lag_eff_from_date AS (
	SELECT
	sup_assoc_prog_code_id,
	eff_from_date,
	eff_to_date AS orig_eff_to_date,
	assoc_prog_code,
	-- *INF*: DECODE(TRUE,
	-- assoc_prog_code = v_prev_prog_code,
	-- ADD_TO_DATE(v_prev_eff_from_date,'SS',-1),orig_eff_to_date)
	DECODE(
	    TRUE,
	    assoc_prog_code = v_prev_prog_code, DATEADD(SECOND,- 1,v_prev_eff_from_date),
	    orig_eff_to_date
	) AS v_eff_to_date,
	v_eff_to_date AS eff_to_date,
	assoc_prog_code AS v_prev_prog_code,
	eff_from_date AS v_prev_eff_from_date,
	0 AS crrnt_snpsht_flag,
	SYSDATE AS modified_dt
	FROM SQ_sup_association_program_code
),
FIL_FirstRowInAKGrouptRowInAKGroup AS (
	SELECT
	sup_assoc_prog_code_id, 
	orig_eff_to_date, 
	eff_to_date, 
	crrnt_snpsht_flag, 
	modified_dt
	FROM EXP_Lag_eff_from_date
	WHERE orig_eff_to_date != eff_to_date
),
UPD_sup_assoc_program_code_id AS (
	SELECT
	sup_assoc_prog_code_id, 
	orig_eff_to_date, 
	eff_to_date, 
	crrnt_snpsht_flag, 
	modified_dt
	FROM FIL_FirstRowInAKGrouptRowInAKGroup
),
TGT_sup_association_program_code_UPDATE AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_association_program_code AS T
	USING UPD_sup_assoc_program_code_id AS S
	ON T.sup_assoc_prog_code_id = S.sup_assoc_prog_code_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snpsht_flag, T.eff_to_date = S.eff_to_date, T.modified_date = S.modified_dt
),