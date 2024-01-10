WITH
SQ_CLAIM_SUPPORT_01_STAGE AS (
	SELECT A.CS01_CODE, A.CS01_CODE_DES 
	FROM
	 @{pipeline().parameters.SOURCE_TABLE_OWNER}.CLAIM_SUPPORT_01_STAGE A
	WHERE A.CS01_TABLE_ID = 'W001'
),
EXP_Default_Values AS (
	SELECT
	CS01_CODE,
	CS01_CODE_DES,
	-- *INF*: iif(isnull(CS01_CODE),'N/A',LTRIM(RTRIM(CS01_CODE)))
	IFF(CS01_CODE IS NULL,
		'N/A',
		LTRIM(RTRIM(CS01_CODE
			)
		)
	) AS RESERVE_CATEGORY_out,
	-- *INF*: iif(isnull(CS01_CODE_DES),'N/A',LTRIM(RTRIM(CS01_CODE_DES)))
	IFF(CS01_CODE_DES IS NULL,
		'N/A',
		LTRIM(RTRIM(CS01_CODE_DES
			)
		)
	) AS RESERVE_CATEGORY_DESCRIPTION_out
	FROM SQ_CLAIM_SUPPORT_01_STAGE
),
LKP_sup_claim_reserve_category AS (
	SELECT
	sup_claim_reserve_ctgry_id,
	reserve_ctgry_code,
	reserve_ctgry_descript,
	in_RESERVE_CATEGORY
	FROM (
		SELECT sup_claim_reserve_category.sup_claim_reserve_ctgry_id as sup_claim_reserve_ctgry_id,
		 sup_claim_reserve_category.reserve_ctgry_descript as reserve_ctgry_descript, 
		LTRIM(RTRIM(sup_claim_reserve_category.reserve_ctgry_code)) as reserve_ctgry_code
		 FROM sup_claim_reserve_category where crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY reserve_ctgry_code ORDER BY sup_claim_reserve_ctgry_id) = 1
),
EXP_Detect_Changes AS (
	SELECT
	LKP_sup_claim_reserve_category.sup_claim_reserve_ctgry_id AS old_sup_claim_reserve_ctgry_id,
	LKP_sup_claim_reserve_category.reserve_ctgry_descript AS old_reserve_ctgry_descript,
	EXP_Default_Values.RESERVE_CATEGORY_out,
	EXP_Default_Values.RESERVE_CATEGORY_DESCRIPTION_out,
	-- *INF*: iif(isnull(old_sup_claim_reserve_ctgry_id),'NEW',
	-- 	iif((ltrim(rtrim(RESERVE_CATEGORY_DESCRIPTION_out)))!= (ltrim(rtrim(old_reserve_ctgry_descript))),
	-- 	'UPDATE',
	-- 	'NOCHANGE'))
	IFF(old_sup_claim_reserve_ctgry_id IS NULL,
		'NEW',
		IFF(( ltrim(rtrim(RESERVE_CATEGORY_DESCRIPTION_out
					)
				) 
			) != ( ltrim(rtrim(old_reserve_ctgry_descript
					)
				) 
			),
			'UPDATE',
			'NOCHANGE'
		)
	) AS v_changed_flag,
	v_changed_flag AS CHANGED_FLAG,
	1 AS crrnt_snpsht_flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	-- *INF*: iif(v_changed_flag='NEW',
	-- 	to_date('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'),sysdate)
	IFF(v_changed_flag = 'NEW',
		to_date('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'
		),
		sysdate
	) AS eff_from_date,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS'
	) AS eff_to_date,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS source_sys_id,
	sysdate AS created_date,
	sysdate AS modified_date
	FROM EXP_Default_Values
	LEFT JOIN LKP_sup_claim_reserve_category
	ON LKP_sup_claim_reserve_category.reserve_ctgry_code = EXP_Default_Values.RESERVE_CATEGORY_out
),
FIL_sup_claim_reserve_category AS (
	SELECT
	RESERVE_CATEGORY_out, 
	RESERVE_CATEGORY_DESCRIPTION_out, 
	CHANGED_FLAG AS changed_flag, 
	crrnt_snpsht_flag, 
	audit_id, 
	eff_from_date, 
	eff_to_date, 
	source_sys_id, 
	created_date, 
	modified_date
	FROM EXP_Detect_Changes
	WHERE changed_flag = 'NEW' or changed_flag = 'UPDATE'
),
sup_claim_reserve_category_Insert AS (
	INSERT INTO sup_claim_reserve_category
	(reserve_ctgry_code, reserve_ctgry_descript, crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date)
	SELECT 
	RESERVE_CATEGORY_out AS RESERVE_CTGRY_CODE, 
	RESERVE_CATEGORY_DESCRIPTION_out AS RESERVE_CTGRY_DESCRIPT, 
	CRRNT_SNPSHT_FLAG, 
	AUDIT_ID, 
	EFF_FROM_DATE, 
	EFF_TO_DATE, 
	SOURCE_SYS_ID, 
	CREATED_DATE, 
	MODIFIED_DATE
	FROM FIL_sup_claim_reserve_category
),
SQ_sup_claim_reserve_category AS (
	SELECT a.sup_claim_reserve_ctgry_id, 
	a.reserve_ctgry_code,
	a.eff_from_date, 
	a.eff_to_date 
	FROM
	 @{pipeline().parameters.TARGET_TABLE_OWNER}. sup_claim_reserve_category a
	WHERE EXISTS(SELECT 1			
			FROM  @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_claim_reserve_category b
			WHERE source_sys_id= '@{pipeline().parameters.SOURCE_SYSTEM_ID}'  AND crrnt_snpsht_flag = 1
			AND a.reserve_ctgry_code = b.reserve_ctgry_code
			GROUP BY reserve_ctgry_code
			HAVING COUNT(*) > 1)
	ORDER BY reserve_ctgry_code, eff_from_date  DESC
	
	--EXISTS Subquery exists to pick AK Groups that have multiple rows with a 12/31/2100 eff_to_date.
	--When this condition occurs this is an indication that we must expire one or more of these rows.
	--WHERE clause is always made up of current snapshot flag and all columns of the AK
	--GROUP BY clause is always on AK
	--HAVING clause stays the same
	
	--ORDER BY of main query orders all rows first by the AK and then by the eff_from_date in a DESC format
	--the descending order is important because this allows us to avoid another lookup and properly apply the eff_to_date by utilizing a local variable to keep track
),
EXP_lag_from_date AS (
	SELECT
	sup_claim_reserve_ctgry_id,
	reserve_ctgry_code,
	eff_from_date,
	eff_to_date AS orig_eff_to_date,
	-- *INF*: DECODE(TRUE,
	-- 	reserve_ctgry_code = v_PREV_ROW_reserve_ctgry_code, ADD_TO_DATE(v_PREV_ROW_eff_from_date,'SS',-1),
	-- 	orig_eff_to_date)
	-- 	
	DECODE(TRUE,
		reserve_ctgry_code = v_PREV_ROW_reserve_ctgry_code, DATEADD(SECOND,- 1,v_PREV_ROW_eff_from_date),
		orig_eff_to_date
	) AS v_eff_to_date,
	v_eff_to_date AS eff_to_date,
	eff_from_date AS v_PREV_ROW_eff_from_date,
	reserve_ctgry_code AS v_PREV_ROW_reserve_ctgry_code,
	0 AS crrnt_snpsht_flag,
	sysdate AS modified_date
	FROM SQ_sup_claim_reserve_category
),
FIL_FirstRowInAkGroup AS (
	SELECT
	sup_claim_reserve_ctgry_id, 
	orig_eff_to_date, 
	eff_to_date, 
	crrnt_snpsht_flag, 
	modified_date
	FROM EXP_lag_from_date
	WHERE orig_eff_to_date <> eff_to_date
),
UPD_sup_claim_reserve_category AS (
	SELECT
	sup_claim_reserve_ctgry_id, 
	eff_to_date, 
	crrnt_snpsht_flag, 
	modified_date
	FROM FIL_FirstRowInAkGroup
),
sup_claim_reserve_category_Update AS (
	MERGE INTO sup_claim_reserve_category AS T
	USING UPD_sup_claim_reserve_category AS S
	ON T.sup_claim_reserve_ctgry_id = S.sup_claim_reserve_ctgry_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snpsht_flag, T.eff_to_date = S.eff_to_date, T.modified_date = S.modified_date
),