WITH
SQ_coverage_category_stage AS (
	SELECT
		cov_ctgry_stage_id,
		cov_category_code,
		cov_category_descript,
		modified_date,
		modified_user_id,
		extract_date,
		source_sys_id
	FROM coverage_category_stage
),
EXP_default AS (
	SELECT
	cov_category_code,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(cov_category_code)))OR IS_SPACES(LTRIM(RTRIM(cov_category_code))) OR LENGTH(LTRIM(RTRIM(cov_category_code))) =0, 'N/A',LTRIM(RTRIM(cov_category_code)))
	IFF(
	    LTRIM(RTRIM(cov_category_code)) IS NULL
	    or LENGTH(LTRIM(RTRIM(cov_category_code)))>0
	    and TRIM(LTRIM(RTRIM(cov_category_code)))=''
	    or LENGTH(LTRIM(RTRIM(cov_category_code))) = 0,
	    'N/A',
	    LTRIM(RTRIM(cov_category_code))
	) AS cov_category_code_out,
	cov_category_descript,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(cov_category_descript))) OR IS_SPACES(LTRIM(RTRIM(cov_category_descript))) OR LENGTH(LTRIM(RTRIM(cov_category_descript))) = 0 ,'N/A' , LTRIM(RTRIM(cov_category_descript)))
	IFF(
	    LTRIM(RTRIM(cov_category_descript)) IS NULL
	    or LENGTH(LTRIM(RTRIM(cov_category_descript)))>0
	    and TRIM(LTRIM(RTRIM(cov_category_descript)))=''
	    or LENGTH(LTRIM(RTRIM(cov_category_descript))) = 0,
	    'N/A',
	    LTRIM(RTRIM(cov_category_descript))
	) AS cov_category_descript_out
	FROM SQ_coverage_category_stage
),
LKP_sup_coverage_category AS (
	SELECT
	sup_cov_ctgry_id,
	cov_ctgry_descript,
	cov_ctgry_code
	FROM (
		SELECT sup_coverage_category.sup_cov_ctgry_id   as sup_cov_ctgry_id, 
		                  sup_coverage_category.cov_ctgry_descript as cov_ctgry_descript,       
		                  sup_coverage_category.cov_ctgry_code       as cov_ctgry_code
		  FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_coverage_category 
		WHERE crrnt_snpsht_flag =1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY cov_ctgry_code ORDER BY sup_cov_ctgry_id DESC) = 1
),
EXP_detect_changes AS (
	SELECT
	LKP_sup_coverage_category.sup_cov_ctgry_id,
	LKP_sup_coverage_category.cov_ctgry_descript AS old_cov_ctgry_descript,
	EXP_default.cov_category_code_out AS cov_ctgry_code_out,
	EXP_default.cov_category_descript_out AS cov_ctgry_descript_out,
	-- *INF*: IIF(ISNULL( sup_cov_ctgry_id), 'NEW', 
	-- IIF(LTRIM(RTRIM( old_cov_ctgry_descript)) != (LTRIM(RTRIM(cov_ctgry_descript_out))) 
	-- 
	-- , 'UPDATE', 'NOCHANGE'))
	IFF(
	    sup_cov_ctgry_id IS NULL, 'NEW',
	    IFF(
	        LTRIM(RTRIM(old_cov_ctgry_descript)) != (LTRIM(RTRIM(cov_ctgry_descript_out))),
	        'UPDATE',
	        'NOCHANGE'
	    )
	) AS v_changed_flag,
	v_changed_flag AS Changed_flag,
	1 AS crrnt_snpsht_flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	-- *INF*: IIF(v_changed_flag = 'NEW', TO_DATE('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'),SYSDATE)
	IFF(
	    v_changed_flag = 'NEW', TO_TIMESTAMP('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'),
	    CURRENT_TIMESTAMP
	) AS eff_from_date,
	-- *INF*:  TO_DATE('12/31/2100 11:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_TIMESTAMP('12/31/2100 11:59:59', 'MM/DD/YYYY HH24:MI:SS') AS eff_to_date,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS source_sys_id,
	SYSDATE AS created_date,
	SYSDATE AS modified_date
	FROM EXP_default
	LEFT JOIN LKP_sup_coverage_category
	ON LKP_sup_coverage_category.cov_ctgry_code = EXP_default.cov_category_code_out
),
FIL_new_update AS (
	SELECT
	cov_ctgry_code_out AS cov_category_code_out, 
	cov_ctgry_descript_out AS cov_category_descript_out, 
	Changed_flag, 
	crrnt_snpsht_flag, 
	audit_id, 
	eff_from_date, 
	eff_to_date, 
	source_sys_id, 
	created_date, 
	modified_date
	FROM EXP_detect_changes
	WHERE Changed_flag = 'NEW' or Changed_flag = 'UPDATE'
),
sup_coverage_category_INS AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_coverage_category
	(crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date, cov_ctgry_code, cov_ctgry_descript)
	SELECT 
	CRRNT_SNPSHT_FLAG, 
	AUDIT_ID, 
	EFF_FROM_DATE, 
	EFF_TO_DATE, 
	SOURCE_SYS_ID, 
	CREATED_DATE, 
	MODIFIED_DATE, 
	cov_category_code_out AS COV_CTGRY_CODE, 
	cov_category_descript_out AS COV_CTGRY_DESCRIPT
	FROM FIL_new_update
),
SQ_sup_coverage_category AS (
	SELECT a.sup_cov_ctgry_id, 
	a.eff_from_date,
	 a.eff_to_date ,
	a.cov_ctgry_code
	FROM
	 @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_coverage_category  a
	
	WHERE EXISTS(SELECT 1			
			FROM  @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_coverage_category b
			WHERE source_sys_id= '@{pipeline().parameters.SOURCE_SYSTEM_ID}'  AND crrnt_snpsht_flag = 1
			AND a.cov_ctgry_code = b.cov_ctgry_code             
			GROUP BY b.cov_ctgry_code
			HAVING COUNT(*) > 1)
	ORDER BY a.cov_ctgry_code, a.eff_from_date  DESC
	
	--EXISTS Subquery exists to pick AK Groups that have multiple rows with a 12/31/2100 eff_to_date.
	--When this condition occurs this is an indication that we must expire one or more of these rows.
	--WHERE clause is always made up of current snapshot flag and all columns of the AK
	--GROUP BY clause is always on AK
	--HAVING clause stays the same
	
	--ORDER BY of main query orders all rows first by the AK and then by the eff_from_date in a DESC format
	--the descending order is important because this allows us to avoid another lookup and properly apply the eff_to_date by utilizing a local variable to keep track
),
EXP_lag_eff_from_date AS (
	SELECT
	sup_cov_ctgry_id,
	cov_ctgry_code,
	eff_from_date,
	eff_to_date AS orig_eff_to_date,
	-- *INF*: DECODE(
	-- TRUE,cov_ctgry_code= v_Prev_row_cov_ctgry_code, 
	-- ADD_TO_DATE(v_prev_eff_from_date,'SS',-1),orig_eff_to_date)
	--  
	DECODE(
	    TRUE,
	    cov_ctgry_code = v_Prev_row_cov_ctgry_code, DATEADD(SECOND,- 1,v_prev_eff_from_date),
	    orig_eff_to_date
	) AS v_eff_to_date,
	v_eff_to_date AS eff_to_date,
	cov_ctgry_code AS v_Prev_row_cov_ctgry_code,
	eff_from_date AS v_prev_eff_from_date,
	0 AS crrnt_snpsht_flag,
	SYSDATE AS modified_date
	FROM SQ_sup_coverage_category
),
FIL_First_row_in_AK_Group AS (
	SELECT
	sup_cov_ctgry_id, 
	orig_eff_to_date, 
	eff_to_date, 
	crrnt_snpsht_flag, 
	modified_date
	FROM EXP_lag_eff_from_date
	WHERE orig_eff_to_date != eff_to_date
),
UPD_eff_from_date AS (
	SELECT
	sup_cov_ctgry_id, 
	eff_to_date, 
	crrnt_snpsht_flag, 
	modified_date
	FROM FIL_First_row_in_AK_Group
),
sup_coverage_category_UPD AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_coverage_category AS T
	USING UPD_eff_from_date AS S
	ON T.sup_cov_ctgry_id = S.sup_cov_ctgry_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snpsht_flag, T.eff_to_date = S.eff_to_date, T.modified_date = S.modified_date
),