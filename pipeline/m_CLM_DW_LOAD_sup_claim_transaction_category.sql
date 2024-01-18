WITH
SQ_CLAIM_SUPPORT_01_STAGE AS (
	SELECT CS01_CODE, 
	CS01_CODE_DES 
	FROM
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.CLAIM_SUPPORT_01_STAGE
	WHERE
	CS01_TABLE_ID = 'C045'
),
EXP_Default_Values AS (
	SELECT
	CS01_CODE AS in_CS01_CODE,
	CS01_CODE_DES AS in_CS01_CODE_DES,
	-- *INF*: IIF(ISNULL(in_CS01_CODE), 
	-- 'N/A',
	-- ltrim(rtrim(in_CS01_CODE)))
	IFF(in_CS01_CODE IS NULL, 'N/A', ltrim(rtrim(in_CS01_CODE))) AS out_CS01_CODE,
	-- *INF*: IIF(ISNULL(in_CS01_CODE_DES),
	-- 'N/A', 
	-- ltrim(rtrim(in_CS01_CODE_DES)))
	IFF(in_CS01_CODE_DES IS NULL, 'N/A', ltrim(rtrim(in_CS01_CODE_DES))) AS out_CS01_CODE_DES
	FROM SQ_CLAIM_SUPPORT_01_STAGE
),
LKP_Transaction_Category AS (
	SELECT
	sup_claim_trans_catetory_id,
	trans_ctgry_code,
	trans_ctgry_descript
	FROM (
		SELECT 
		a.sup_claim_trans_catetory_id as sup_claim_trans_catetory_id, a.trans_ctgry_descript as trans_ctgry_descript, 
		a.trans_ctgry_code as trans_ctgry_code 
		FROM 
		@{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_claim_transaction_category a
		WHERE a.sup_claim_trans_catetory_id
		in (select MAX(b. sup_claim_trans_catetory_id )
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_claim_transaction_category  b
		WHERE crrnt_snpsht_flag=1
			GROUP BY b.trans_ctgry_code)
		ORDER BY trans_ctgry_code
		
		
		
		--ORDER BY clause is always the AK.  When any comments exist in the SQL override Informatica will no longer generate an ORDER BY statement
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY trans_ctgry_code ORDER BY sup_claim_trans_catetory_id) = 1
),
EXP_DetectChanges AS (
	SELECT
	LKP_Transaction_Category.sup_claim_trans_catetory_id AS old_sup_claim_trans_catetory_id,
	LKP_Transaction_Category.trans_ctgry_descript AS old_trans_ctgry_descript,
	EXP_Default_Values.out_CS01_CODE,
	EXP_Default_Values.out_CS01_CODE_DES,
	-- *INF*: iif(isnull(old_sup_claim_trans_catetory_id),'NEW',
	-- 	iif((out_CS01_CODE_DES<> old_trans_ctgry_descript),
	-- 	'UPDATE',
	-- 	'NOCHANGE'))
	IFF(
	    old_sup_claim_trans_catetory_id IS NULL, 'NEW',
	    IFF(
	        (out_CS01_CODE_DES <> old_trans_ctgry_descript), 'UPDATE', 'NOCHANGE'
	    )
	) AS V_changed_flag,
	V_changed_flag AS changed_flag,
	1 AS crrnt_snpsht_flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	-- *INF*: iif(V_changed_flag='NEW',
	-- 	to_date('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'),sysdate)
	-- 
	-- 
	IFF(
	    V_changed_flag = 'NEW', TO_TIMESTAMP('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'),
	    CURRENT_TIMESTAMP
	) AS eff_from_date,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	-- 
	TO_TIMESTAMP('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS eff_to_date,
	sysdate AS created_date,
	sysdate AS modified_date,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS Source_Sys_Id
	FROM EXP_Default_Values
	LEFT JOIN LKP_Transaction_Category
	ON LKP_Transaction_Category.trans_ctgry_code = EXP_Default_Values.out_CS01_CODE
),
FIL_Transaction_Category AS (
	SELECT
	changed_flag, 
	out_CS01_CODE, 
	out_CS01_CODE_DES, 
	crrnt_snpsht_flag, 
	audit_id, 
	eff_from_date, 
	eff_to_date, 
	created_date, 
	modified_date, 
	Source_Sys_Id
	FROM EXP_DetectChanges
	WHERE changed_flag = 'NEW' or changed_flag = 'UPDATE'
),
sup_claim_transaction_category_insert AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_claim_transaction_category
	(trans_ctgry_code, trans_ctgry_descript, crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date)
	SELECT 
	out_CS01_CODE AS TRANS_CTGRY_CODE, 
	out_CS01_CODE_DES AS TRANS_CTGRY_DESCRIPT, 
	CRRNT_SNPSHT_FLAG, 
	AUDIT_ID, 
	EFF_FROM_DATE, 
	EFF_TO_DATE, 
	Source_Sys_Id AS SOURCE_SYS_ID, 
	CREATED_DATE, 
	MODIFIED_DATE
	FROM FIL_Transaction_Category
),
SQ_sup_claim_transaction_category AS (
	SELECT 
	a.sup_claim_trans_catetory_id, a.trans_ctgry_code ,
	a.eff_from_date, a.eff_to_date 
	FROM
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_claim_transaction_category a 
	WHERE EXISTS(SELECT 1			
	FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_claim_transaction_category  b	
	WHERE crrnt_snpsht_flag=1
		AND a.trans_ctgry_code  = b.trans_ctgry_code 
		GROUP BY trans_ctgry_code 
		HAVING COUNT(*) > 1)
	ORDER BY trans_ctgry_code , eff_from_date  DESC
	
	--EXISTS Subquery exists to pick AK Groups that have multiple rows with a 12/31/2100 eff_to_date.
	--When this condition occurs this is an indication that we must expire one or more of these rows.
	--WHERE clause is always made up of current snapshot flag and all columns of the AK
	--GROUP BY clause is always on AK
	--HAVING clause stays the same
	
	--ORDER BY of main query orders all rows first by the AK and then by the eff_from_date in a DESC format
	--the descending order is important because this allows us to avoid another lookup and properly apply the eff_to_date by utilizing a local variable to keep track
),
EXP_Lag_Eff_from_date AS (
	SELECT
	sup_claim_trans_catetory_id AS sup_trans_catetory_id,
	trans_ctgry_code,
	eff_from_date,
	eff_to_date AS orig_eff_to_date,
	-- *INF*: DECODE(TRUE,
	-- 	trans_ctgry_code = v_PREV_ROW_trans_ctgry_code, ADD_TO_DATE(v_PREV_ROW_eff_from_date,'SS',-1),
	-- 	orig_eff_to_date)
	-- 	
	DECODE(
	    TRUE,
	    trans_ctgry_code = v_PREV_ROW_trans_ctgry_code, DATEADD(SECOND,- 1,v_PREV_ROW_eff_from_date),
	    orig_eff_to_date
	) AS v_eff_to_date,
	v_eff_to_date AS eff_to_date,
	eff_from_date AS v_PREV_ROW_eff_from_date,
	trans_ctgry_code AS v_PREV_ROW_trans_ctgry_code,
	0 AS crrnt_snpsht_flag,
	sysdate AS modified_date
	FROM SQ_sup_claim_transaction_category
),
FIL_FirstRowInAKGroup AS (
	SELECT
	sup_trans_catetory_id, 
	orig_eff_to_date, 
	eff_to_date, 
	crrnt_snpsht_flag, 
	modified_date
	FROM EXP_Lag_Eff_from_date
	WHERE orig_eff_to_date <> eff_to_date
),
UPD_sup_transaction_category AS (
	SELECT
	sup_trans_catetory_id, 
	eff_to_date, 
	crrnt_snpsht_flag, 
	modified_date
	FROM FIL_FirstRowInAKGroup
),
sup_claim_transaction_category_update AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_claim_transaction_category AS T
	USING UPD_sup_transaction_category AS S
	ON T.sup_claim_trans_catetory_id = S.sup_trans_catetory_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snpsht_flag, T.eff_to_date = S.eff_to_date, T.modified_date = S.modified_date
),