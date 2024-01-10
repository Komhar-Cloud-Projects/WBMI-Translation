WITH
SQ_CLAIM_SUPPORT_01_STAGE AS (
	SELECT CS01_CODE, 
	CS01_CODE_DES 
	FROM
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.CLAIM_SUPPORT_01_STAGE
	WHERE
	CS01_TABLE_ID = 'C010'
),
Exp_Default_Values AS (
	SELECT
	CS01_CODE AS in_CS01_CODE,
	CS01_CODE_DES AS in_CS01_CODE_DES,
	-- *INF*: IIF(ISNULL(in_CS01_CODE), 
	-- 'N/A',
	-- (rpad(in_CS01_CODE,4)))
	IFF(in_CS01_CODE IS NULL, 'N/A', ( rpad(in_CS01_CODE, 4) )) AS out_CS01_CODE,
	-- *INF*: IIF(ISNULL(in_CS01_CODE_DES),
	-- 'N/A', 
	-- ltrim(rtrim(in_CS01_CODE_DES)))
	IFF(in_CS01_CODE_DES IS NULL, 'N/A', ltrim(rtrim(in_CS01_CODE_DES))) AS out_CS01_CODE_DES
	FROM SQ_CLAIM_SUPPORT_01_STAGE
),
LKP_sup_claim_party_role_code AS (
	SELECT
	sup_claim_party_role_code_id,
	claim_party_role_code,
	claim_party_role_descript
	FROM (
		SELECT 
		a.sup_claim_party_role_code_id as sup_claim_party_role_code_id, 
		a.claim_party_role_descript as claim_party_role_descript, a.claim_party_role_code as claim_party_role_code FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_claim_party_role_code a
		WHERE a.sup_claim_party_role_code_id 
		in (select MAX(b. sup_claim_party_role_code_id )
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_claim_party_role_code b
		WHERE crrnt_snpsht_flag=1
			GROUP BY b.claim_party_role_code)
		ORDER BY claim_party_role_code
		
		--IN Subquery exists so that we only pick the MAX value of the PK for each AK Group
		--WHERE clause is always eff_to_date = '12/31/2100'
		--GROUP BY clause is always the AK
		--ORDER BY clause is always the AK.  When any comments exist in the SQL override Informatica will no longer generate an ORDER BY statement
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_party_role_code ORDER BY sup_claim_party_role_code_id) = 1
),
EXP_Detect_changes AS (
	SELECT
	LKP_sup_claim_party_role_code.sup_claim_party_role_code_id AS old_sup_claim_party_role_code_id,
	LKP_sup_claim_party_role_code.claim_party_role_descript AS old_claim_party_role_descript,
	Exp_Default_Values.out_CS01_CODE_DES,
	-- *INF*: iif(isnull(old_sup_claim_party_role_code_id),'NEW',
	-- 	iif((out_CS01_CODE_DES <> old_claim_party_role_descript),
	-- 	'UPDATE',
	-- 	'NOCHANGE'))
	IFF(old_sup_claim_party_role_code_id IS NULL, 'NEW', IFF(( out_CS01_CODE_DES <> old_claim_party_role_descript ), 'UPDATE', 'NOCHANGE')) AS v_changed_flag,
	Exp_Default_Values.out_CS01_CODE,
	v_changed_flag AS changed_flag,
	1 AS crrnt_snpsht_flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	-- *INF*: iif(v_changed_flag='NEW',
	-- 	to_date('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'),sysdate)
	-- 
	-- --sysdate normally has a time value.  We don't want the time value as our effectivity runs from day to day starting at midnight
	IFF(v_changed_flag = 'NEW', to_date('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'), sysdate) AS eff_from_date,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS eff_to_date,
	sysdate AS created_date,
	sysdate AS modified_date,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS source_system_id
	FROM Exp_Default_Values
	LEFT JOIN LKP_sup_claim_party_role_code
	ON LKP_sup_claim_party_role_code.claim_party_role_code = Exp_Default_Values.out_CS01_CODE
),
FIL_sup_claim_party_role_code_insert AS (
	SELECT
	out_CS01_CODE, 
	out_CS01_CODE_DES, 
	changed_flag, 
	crrnt_snpsht_flag, 
	audit_id, 
	eff_from_date, 
	eff_to_date, 
	created_date, 
	modified_date, 
	source_system_id
	FROM EXP_Detect_changes
	WHERE changed_flag = 'NEW' or changed_flag = 'UPDATE'
),
sup_claim_party_role_code_insert AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_claim_party_role_code
	(claim_party_role_code, claim_party_role_descript, crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date)
	SELECT 
	out_CS01_CODE AS CLAIM_PARTY_ROLE_CODE, 
	out_CS01_CODE_DES AS CLAIM_PARTY_ROLE_DESCRIPT, 
	CRRNT_SNPSHT_FLAG, 
	AUDIT_ID, 
	EFF_FROM_DATE, 
	EFF_TO_DATE, 
	source_system_id AS SOURCE_SYS_ID, 
	CREATED_DATE, 
	MODIFIED_DATE
	FROM FIL_sup_claim_party_role_code_insert
),
SQ_sup_claim_party_role_code AS (
	SELECT 
	a.sup_claim_party_role_code_id,
	a.claim_party_role_code, 
	a.eff_from_date, 
	a.eff_to_date 
	FROM
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_claim_party_role_code a 
	WHERE EXISTS(SELECT 1			
	FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_claim_party_role_code  b	
	WHERE crrnt_snpsht_flag = 1
		AND a.claim_party_role_code = b.claim_party_role_code
		GROUP BY claim_party_role_code
		HAVING COUNT(*) > 1)
	ORDER BY claim_party_role_code, eff_from_date  DESC
	
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
	sup_claim_party_role_code_id,
	claim_party_role_code,
	eff_from_date,
	eff_to_date AS orig_eff_to_date,
	-- *INF*: DECODE(TRUE,
	-- 	claim_party_role_code = v_PREV_ROW_claim_party_role_code, ADD_TO_DATE(v_PREV_ROW_eff_from_date,'SS',-1),
	-- 	orig_eff_to_date)
	-- 	
	DECODE(TRUE,
		claim_party_role_code = v_PREV_ROW_claim_party_role_code, ADD_TO_DATE(v_PREV_ROW_eff_from_date, 'SS', - 1),
		orig_eff_to_date) AS v_eff_to_date,
	v_eff_to_date AS eff_to_date,
	eff_from_date AS v_PREV_ROW_eff_from_date,
	claim_party_role_code AS v_PREV_ROW_claim_party_role_code,
	0 AS crrnt_snpsht_flag,
	sysdate AS modified_date
	FROM SQ_sup_claim_party_role_code
),
FIL_FirstRowInAKGroup AS (
	SELECT
	sup_claim_party_role_code_id, 
	orig_eff_to_date, 
	eff_to_date, 
	crrnt_snpsht_flag, 
	modified_date
	FROM EXP_Lag_eff_from_date
	WHERE orig_eff_to_date <> eff_to_date
),
UPD_sup_claim_party_role_code AS (
	SELECT
	sup_claim_party_role_code_id, 
	eff_to_date, 
	crrnt_snpsht_flag, 
	modified_date
	FROM FIL_FirstRowInAKGroup
),
sup_claim_party_role_code_update AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_claim_party_role_code AS T
	USING UPD_sup_claim_party_role_code AS S
	ON T.sup_claim_party_role_code_id = S.sup_claim_party_role_code_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snpsht_flag, T.eff_to_date = S.eff_to_date, T.modified_date = S.modified_date
),