WITH
SQ_CLAIM_SUPPORT_01_STAGE AS (
	SELECT CS01_CODE, 
	CS01_CODE_DES 
	FROM
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.CLAIM_SUPPORT_01_STAGE
	WHERE
	CS01_TABLE_ID = 'C188'
),
EXP_Default_Values AS (
	SELECT
	CS01_CODE AS in_CS01_CODE,
	CS01_CODE_DES AS in_CS01_CODE_DES,
	-- *INF*: iif(isnull(in_CS01_CODE),'N/A',in_CS01_CODE)
	IFF(in_CS01_CODE IS NULL,
		'N/A',
		in_CS01_CODE
	) AS out_CS01_CODE,
	-- *INF*: iif(isnull(in_CS01_CODE_DES),'N/A',in_CS01_CODE_DES)
	IFF(in_CS01_CODE_DES IS NULL,
		'N/A',
		in_CS01_CODE_DES
	) AS out_CS01_CODE_DES
	FROM SQ_CLAIM_SUPPORT_01_STAGE
),
LKP_sup_claim_subrogation_deductible_status_code AS (
	SELECT
	sup_claim_subrogation_ded_status_code_id,
	ded_status_code,
	ded_status_code_descript,
	out_CS01_CODE
	FROM (
		SELECT sup_claim_subrogation_deductible_status_code.sup_claim_subrogation_ded_status_code_id as sup_claim_subrogation_ded_status_code_id, sup_claim_subrogation_deductible_status_code.ded_status_code_descript as ded_status_code_descript, sup_claim_subrogation_deductible_status_code.ded_status_code as ded_status_code FROM sup_claim_subrogation_deductible_status_code
		WHERE crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ded_status_code ORDER BY sup_claim_subrogation_ded_status_code_id) = 1
),
EXP_lkpvalues AS (
	SELECT
	LKP_sup_claim_subrogation_deductible_status_code.sup_claim_subrogation_ded_status_code_id AS old_sup_claim_subrogation_ded_tatus_code_id,
	LKP_sup_claim_subrogation_deductible_status_code.ded_status_code_descript AS old_ded_status_code_descript,
	EXP_Default_Values.out_CS01_CODE,
	EXP_Default_Values.out_CS01_CODE_DES,
	-- *INF*: iif(isnull(old_sup_claim_subrogation_ded_tatus_code_id),'NEW',
	-- 	iif((out_CS01_CODE_DES <>old_ded_status_code_descript),
	-- 	'UPDATE',
	-- 	'NOCHANGE'))
	IFF(old_sup_claim_subrogation_ded_tatus_code_id IS NULL,
		'NEW',
		IFF(( out_CS01_CODE_DES <> old_ded_status_code_descript 
			),
			'UPDATE',
			'NOCHANGE'
		)
	) AS V_CHANGED_FLAG,
	V_CHANGED_FLAG AS changed_flag,
	1 AS crrnt_snpsht_flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS auidt_id,
	-- *INF*: iif(V_CHANGED_FLAG='NEW',
	-- 	to_date('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'),sysdate)
	IFF(V_CHANGED_FLAG = 'NEW',
		to_date('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'
		),
		sysdate
	) AS eff_from_date,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS'
	) AS eff_to_date,
	sysdate AS created_date,
	sysdate AS modified_date,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS Source_sys_id
	FROM EXP_Default_Values
	LEFT JOIN LKP_sup_claim_subrogation_deductible_status_code
	ON LKP_sup_claim_subrogation_deductible_status_code.ded_status_code = EXP_Default_Values.out_CS01_CODE
),
FIL_Sup_transaction_code AS (
	SELECT
	out_CS01_CODE, 
	out_CS01_CODE_DES, 
	crrnt_snpsht_flag, 
	auidt_id, 
	eff_from_date, 
	eff_to_date, 
	created_date, 
	modified_date, 
	Source_sys_id, 
	changed_flag
	FROM EXP_lkpvalues
	WHERE changed_flag = 'NEW' or changed_flag = 'UPDATE'
),
sup_claim_subrogation_deductible_status_code AS (
	INSERT INTO sup_claim_subrogation_deductible_status_code
	(crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date, ded_status_code, ded_status_code_descript)
	SELECT 
	CRRNT_SNPSHT_FLAG, 
	auidt_id AS AUDIT_ID, 
	EFF_FROM_DATE, 
	EFF_TO_DATE, 
	Source_sys_id AS SOURCE_SYS_ID, 
	CREATED_DATE, 
	MODIFIED_DATE, 
	out_CS01_CODE AS DED_STATUS_CODE, 
	out_CS01_CODE_DES AS DED_STATUS_CODE_DESCRIPT
	FROM FIL_Sup_transaction_code
),
SQ_sup_claim_subrogation_deductible_status_code AS (
	SELECT a.sup_claim_subrogation_ded_status_code_id, 
	a.eff_from_date, 
	a.eff_to_date, 
	a.ded_status_code 
	FROM
	 sup_claim_subrogation_deductible_status_code a 
	WHERE EXISTS(SELECT 1			
	FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_claim_subrogation_deductible_status_code  b
	WHERE eff_to_date = '12/31/2100 23:59:59'
		AND a.ded_status_code = b.ded_status_code
		GROUP BY ded_status_code 
		HAVING COUNT(*) > 1)
	ORDER BY ded_status_code , eff_from_date  DESC
	
	--EXISTS Subquery exists to pick AK Groups that have multiple rows with a 12/31/2100 eff_to_date.
	--When this condition occurs this is an indication that we must expire one or more of these rows.
	--WHERE clause is always made up of current snapshot flag and all columns of the AK
	--GROUP BY clause is always on AK
	--HAVING clause stays the same
	
	--ORDER BY of main query orders all rows first by the AK and then by the eff_from_date in a DESC format
	--the descending order is important because this allows us to avoid another lookup and properly apply the eff_to_date by utilizing a local variable to keep track
),
EXP_Lag_eff_from_date AS (
	SELECT
	sup_claim_subrogation_ded_tatus_code_id,
	ded_status_code,
	eff_from_date,
	eff_to_date AS orig_eff_to_date,
	-- *INF*: DECODE(TRUE,
	-- 	ded_status_code = V_PREV_ROW_ded_status_code, ADD_TO_DATE(V_PREV_ROW_eff_from_date,'SS',-1),
	-- 	orig_eff_to_date)
	DECODE(TRUE,
		ded_status_code = V_PREV_ROW_ded_status_code, DATEADD(SECOND,- 1,V_PREV_ROW_eff_from_date),
		orig_eff_to_date
	) AS v_eff_to_date,
	v_eff_to_date AS eff_to_date,
	eff_from_date AS V_PREV_ROW_eff_from_date,
	ded_status_code AS V_PREV_ROW_ded_status_code,
	0 AS crrnt_snpsht_flag,
	sysdate AS modified_date
	FROM SQ_sup_claim_subrogation_deductible_status_code
),
FIL_FIRST_ROW_IN_AK_ID AS (
	SELECT
	sup_claim_subrogation_ded_tatus_code_id, 
	orig_eff_to_date, 
	eff_to_date, 
	crrnt_snpsht_flag, 
	modified_date
	FROM EXP_Lag_eff_from_date
	WHERE orig_eff_to_date <> eff_to_date
),
UPD_sup_claim_subrogation_deductible_status_code AS (
	SELECT
	sup_claim_subrogation_ded_tatus_code_id, 
	eff_to_date, 
	crrnt_snpsht_flag, 
	modified_date
	FROM FIL_FIRST_ROW_IN_AK_ID
),
sup_claim_subrogation_deductible_status_code2 AS (
	MERGE INTO sup_claim_subrogation_deductible_status_code AS T
	USING UPD_sup_claim_subrogation_deductible_status_code AS S
	ON T.sup_claim_subrogation_ded_tatus_code_id = S.sup_claim_subrogation_ded_tatus_code_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snpsht_flag, T.eff_to_date = S.eff_to_date, T.modified_date = S.modified_date
),