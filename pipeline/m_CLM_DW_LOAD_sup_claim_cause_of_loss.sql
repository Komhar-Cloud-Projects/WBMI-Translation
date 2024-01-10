WITH
SQ_CLAIM_SUPPORT_01_STAGE AS (
	SELECT DISTINCT a.CS01_CODE, a.CS01_CODE_DES 
	FROM
	 @{pipeline().parameters.SOURCE_TABLE_OWNER}.CLAIM_SUPPORT_01_STAGE a 
	WHERE a.CS01_TABLE_ID =  'C047'
),
EXP_Default_Values AS (
	SELECT
	CS01_CODE,
	CS01_CODE_DES,
	-- *INF*: iif(isnull(CS01_CODE),'N/A',SUBSTR(LTRIM(RTRIM(CS01_CODE)),1,3))
	IFF(CS01_CODE IS NULL, 'N/A', SUBSTR(LTRIM(RTRIM(CS01_CODE)), 1, 3)) AS v_major_peril_code,
	v_major_peril_code AS major_peril_code_out,
	-- *INF*: iif(isnull(CS01_CODE),'N/A',SUBSTR(LTRIM(RTRIM(CS01_CODE)),5,2))
	IFF(CS01_CODE IS NULL, 'N/A', SUBSTR(LTRIM(RTRIM(CS01_CODE)), 5, 2)) AS v_cause_of_loss_code,
	v_cause_of_loss_code AS cause_of_loss_code_out,
	-- *INF*: iif(isnull(CS01_CODE),'N/A',SUBSTR(LTRIM(RTRIM(CS01_CODE)),7,1))
	IFF(CS01_CODE IS NULL, 'N/A', SUBSTR(LTRIM(RTRIM(CS01_CODE)), 7, 1)) AS v_reserve_ctgry_code,
	v_reserve_ctgry_code AS reserve_ctgry_code_out,
	-- *INF*: iif(isnull(CS01_CODE_DES),'N/A',LTRIM(RTRIM(CS01_CODE_DES)))
	IFF(CS01_CODE_DES IS NULL, 'N/A', LTRIM(RTRIM(CS01_CODE_DES))) AS v_cause_of_loss_description,
	v_cause_of_loss_description AS cause_of_loss_description
	FROM SQ_CLAIM_SUPPORT_01_STAGE
),
LKP_Sup_Claim_cause_of_loss AS (
	SELECT
	sup_claim_cause_of_loss_id,
	cause_of_loss_long_descript,
	major_peril_code,
	cause_of_loss_code,
	reserve_ctgry_code
	FROM (
		SELECT 
			sup_claim_cause_of_loss_id,
			cause_of_loss_long_descript,
			major_peril_code,
			cause_of_loss_code,
			reserve_ctgry_code
		FROM sup_claim_cause_of_loss
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY major_peril_code,cause_of_loss_code,reserve_ctgry_code ORDER BY sup_claim_cause_of_loss_id) = 1
),
EXP_detect_changes AS (
	SELECT
	LKP_Sup_Claim_cause_of_loss.sup_claim_cause_of_loss_id,
	LKP_Sup_Claim_cause_of_loss.cause_of_loss_long_descript AS old_cause_of_loss_long_descript,
	EXP_Default_Values.major_peril_code_out,
	EXP_Default_Values.cause_of_loss_code_out,
	EXP_Default_Values.reserve_ctgry_code_out,
	EXP_Default_Values.cause_of_loss_description,
	-- *INF*: IIF(ISNULL(sup_claim_cause_of_loss_id), 'NEW', 
	-- 	IIF(ltrim(rtrim(old_cause_of_loss_long_descript)) != ltrim(rtrim(cause_of_loss_description)), 
	-- 'UPDATE',
	--  'NOCHANGE'))
	-- 
	IFF(sup_claim_cause_of_loss_id IS NULL, 'NEW', IFF(ltrim(rtrim(old_cause_of_loss_long_descript)) != ltrim(rtrim(cause_of_loss_description)), 'UPDATE', 'NOCHANGE')) AS v_CHANGED_FLAG,
	v_CHANGED_FLAG AS CHANGED_FLAG,
	1 AS crrnt_snpsht_flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	-- *INF*: iif(v_CHANGED_FLAG='NEW',
	-- 	to_date('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'),sysdate)
	IFF(v_CHANGED_FLAG = 'NEW', to_date('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'), sysdate) AS eff_from_date,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS eff_to_date,
	SYSDATE AS created_date,
	SYSDATE AS modified_date,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS source_sys_id
	FROM EXP_Default_Values
	LEFT JOIN LKP_Sup_Claim_cause_of_loss
	ON LKP_Sup_Claim_cause_of_loss.major_peril_code = EXP_Default_Values.major_peril_code_out AND LKP_Sup_Claim_cause_of_loss.cause_of_loss_code = EXP_Default_Values.cause_of_loss_code_out AND LKP_Sup_Claim_cause_of_loss.reserve_ctgry_code = EXP_Default_Values.reserve_ctgry_code_out
),
FIL_sup_claim_cause_of_loss_insert AS (
	SELECT
	major_peril_code_out, 
	cause_of_loss_code_out, 
	reserve_ctgry_code_out, 
	cause_of_loss_description, 
	CHANGED_FLAG, 
	crrnt_snpsht_flag, 
	audit_id, 
	eff_from_date, 
	eff_to_date, 
	created_date, 
	modified_date, 
	source_sys_id
	FROM EXP_detect_changes
	WHERE CHANGED_FLAG='NEW' or CHANGED_FLAG='UPDATE'
),
sup_claim_cause_of_loss_Insert AS (
	INSERT INTO sup_claim_cause_of_loss
	(major_peril_code, cause_of_loss_code, reserve_ctgry_code, cause_of_loss_long_descript, crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date)
	SELECT 
	major_peril_code_out AS MAJOR_PERIL_CODE, 
	cause_of_loss_code_out AS CAUSE_OF_LOSS_CODE, 
	reserve_ctgry_code_out AS RESERVE_CTGRY_CODE, 
	cause_of_loss_description AS CAUSE_OF_LOSS_LONG_DESCRIPT, 
	CRRNT_SNPSHT_FLAG, 
	AUDIT_ID, 
	EFF_FROM_DATE, 
	EFF_TO_DATE, 
	SOURCE_SYS_ID, 
	CREATED_DATE, 
	MODIFIED_DATE
	FROM FIL_sup_claim_cause_of_loss_insert
),
SQ_sup_claim_cause_of_loss AS (
	SELECT a.sup_claim_cause_of_loss_id, 
	a.cause_of_loss_code,
	a.reserve_ctgry_code,
	a.major_peril_code,
	a.eff_from_date, 
	a.eff_to_date 
	FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_claim_cause_of_loss a
	WHERE EXISTS(SELECT 1			
			FROM  @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_claim_cause_of_loss b
			WHERE source_sys_id= '@{pipeline().parameters.SOURCE_SYSTEM_ID}'  AND crrnt_snpsht_flag = 1
			AND a.cause_of_loss_code = b.cause_of_loss_code
	             AND a.reserve_ctgry_code =b.reserve_ctgry_code
	             AND a.major_peril_code = b.major_peril_code
			GROUP BY cause_of_loss_code,reserve_ctgry_code,major_peril_code
			HAVING COUNT(*) > 1)
	ORDER BY cause_of_loss_code, eff_from_date  DESC
	
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
	sup_claim_cause_of_loss_id,
	major_peril_code,
	cause_of_loss_code,
	reserve_ctgry_code,
	eff_from_date,
	eff_to_date AS orig_eff_to_date,
	-- *INF*: DECODE(TRUE,
	-- 	cause_of_loss_code = v_prev_row_cause_of_loss_code AND
	--       reserve_ctgry_code  = v_prev_row_reserve_ctgry_code AND
	--       major_peril_code = v_prev_row_major_peril_code ,
	--  ADD_TO_DATE(v_prev_eff_from_date,'SS',-1),
	-- 	orig_eff_to_date)
	-- 	
	DECODE(TRUE,
		cause_of_loss_code = v_prev_row_cause_of_loss_code AND reserve_ctgry_code = v_prev_row_reserve_ctgry_code AND major_peril_code = v_prev_row_major_peril_code, ADD_TO_DATE(v_prev_eff_from_date, 'SS', - 1),
		orig_eff_to_date) AS v_eff_to_date,
	v_eff_to_date AS eff_to_date,
	major_peril_code AS v_prev_row_major_peril_code,
	cause_of_loss_code AS v_prev_row_cause_of_loss_code,
	reserve_ctgry_code AS v_prev_row_reserve_ctgry_code,
	eff_from_date AS v_prev_eff_from_date,
	0 AS crrnt_snpsht_flag,
	SYSDATE AS modified_date
	FROM SQ_sup_claim_cause_of_loss
),
FIL_First_rown_inAKGroup AS (
	SELECT
	sup_claim_cause_of_loss_id, 
	orig_eff_to_date, 
	eff_to_date, 
	crrnt_snpsht_flag, 
	modified_date
	FROM EXP_lag_eff_from_date
	WHERE orig_eff_to_date != eff_to_date
),
UPD_sup_claim_cause_of_loss AS (
	SELECT
	sup_claim_cause_of_loss_id, 
	eff_to_date, 
	crrnt_snpsht_flag, 
	modified_date
	FROM FIL_First_rown_inAKGroup
),
sup_claim_cause_of_loss_Update AS (
	MERGE INTO sup_claim_cause_of_loss AS T
	USING UPD_sup_claim_cause_of_loss AS S
	ON T.sup_claim_cause_of_loss_id = S.sup_claim_cause_of_loss_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snpsht_flag, T.eff_to_date = S.eff_to_date, T.modified_date = S.modified_date
),