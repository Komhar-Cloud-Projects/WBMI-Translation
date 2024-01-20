WITH
SQ_SUP_CHANGE_REASON_STAGE AS (
	SELECT
		SUP_CHANGE_REASON_ID,
		REASON_CODE,
		REASON_DESC,
		MODIFIED_DATE,
		MODIFIED_USER_ID,
		EXTRACT_DATE,
		AS_OF_DATE,
		RECORD_COUNT,
		SOURCE_SYSTEM_ID
	FROM SUP_CHANGE_REASON_STAGE
),
EXP_default_values AS (
	SELECT
	REASON_CODE AS in_REASON_CODE,
	REASON_DESC AS in_REASON_DESC,
	-- *INF*: iif(isnull(in_REASON_CODE),'NA',in_REASON_CODE)
	IFF(in_REASON_CODE IS NULL, 'NA', in_REASON_CODE) AS out_reason_code,
	-- *INF*: iif(isnull(in_REASON_DESC),'N/A',in_REASON_DESC)
	IFF(in_REASON_DESC IS NULL, 'N/A', in_REASON_DESC) AS out_reason_desc
	FROM SQ_SUP_CHANGE_REASON_STAGE
),
LKP_sup_transaction_reason AS (
	SELECT
	sup_claim_trans_rsn_id,
	trans_rsn_descript,
	trans_rsn_code
	FROM (
		SELECT 
		a.sup_claim_trans_rsn_id as sup_claim_trans_rsn_id, 
		a.trans_rsn_descript as trans_rsn_descript, 
		a.trans_rsn_code as trans_rsn_code 
		FROM 
		@{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_claim_transaction_reason a
		WHERE a.sup_claim_trans_rsn_id
		in (select MAX(b. sup_claim_trans_rsn_id )
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_claim_transaction_reason  b
		WHERE crrnt_snpsht_flag=1
			GROUP BY b.trans_rsn_code)
		ORDER BY trans_rsn_code
		
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY trans_rsn_code ORDER BY sup_claim_trans_rsn_id) = 1
),
EXP_lkp_Values AS (
	SELECT
	LKP_sup_transaction_reason.sup_claim_trans_rsn_id AS old_sup_claim_trans_rsn_id,
	LKP_sup_transaction_reason.trans_rsn_descript AS old_trans_rsn_descript,
	EXP_default_values.out_reason_code,
	EXP_default_values.out_reason_desc,
	-- *INF*: iif(isnull(old_sup_claim_trans_rsn_id),'NEW',
	-- 	iif((old_trans_rsn_descript<> out_reason_desc),
	-- 	'UPDATE',
	-- 	'NOCHANGE'))
	IFF(
	    old_sup_claim_trans_rsn_id IS NULL, 'NEW',
	    IFF(
	        (old_trans_rsn_descript <> out_reason_desc), 'UPDATE', 'NOCHANGE'
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
	sysdate AS created_date,
	sysdate AS modified_date,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS Source_sys_id
	FROM EXP_default_values
	LEFT JOIN LKP_sup_transaction_reason
	ON LKP_sup_transaction_reason.trans_rsn_code = EXP_default_values.out_reason_code
),
FIL_sup_transaction_reason AS (
	SELECT
	out_reason_code, 
	out_reason_desc, 
	changed_flag, 
	crrnt_snpsht_flag, 
	audit_id, 
	eff_from_date, 
	eff_to_date, 
	created_date, 
	modified_date, 
	Source_sys_id
	FROM EXP_lkp_Values
	WHERE changed_flag = 'NEW' or changed_flag = 'UPDATE'
),
sup_claims_transaction_reason_insert AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_claim_transaction_reason
	(trans_rsn_code, trans_rsn_descript, crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date)
	SELECT 
	out_reason_code AS TRANS_RSN_CODE, 
	out_reason_desc AS TRANS_RSN_DESCRIPT, 
	CRRNT_SNPSHT_FLAG, 
	AUDIT_ID, 
	EFF_FROM_DATE, 
	EFF_TO_DATE, 
	Source_sys_id AS SOURCE_SYS_ID, 
	CREATED_DATE, 
	MODIFIED_DATE
	FROM FIL_sup_transaction_reason
),
SQ_sup_transaction_reason AS (
	SELECT 
	a.sup_claim_trans_rsn_id, a.trans_rsn_code, 
	a.eff_from_date, a.eff_to_date
	FROM
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_claim_transaction_reason a
	WHERE EXISTS(SELECT 1			
	FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}. sup_claim_transaction_reason  b	
	WHERE eff_to_date = '12/31/2100 23:59:59'
		AND a.trans_rsn_code = b.trans_rsn_code
		GROUP BY trans_rsn_code
		HAVING COUNT(*) > 1)
	ORDER BY trans_rsn_code, eff_from_date  DESC
	
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
	sup_trans_rsn_id,
	trans_rsn_code,
	eff_from_date,
	eff_to_date AS orig_eff_to_date,
	-- *INF*: DECODE(TRUE,
	-- 	trans_rsn_code = v_PREV_ROW_trans_rsn_code, ADD_TO_DATE(v_PREV_ROW_EFF_FROM_DATE,'SS',-1),
	-- 	orig_eff_to_date)
	DECODE(
	    TRUE,
	    trans_rsn_code = v_PREV_ROW_trans_rsn_code, DATEADD(SECOND,- 1,v_PREV_ROW_EFF_FROM_DATE),
	    orig_eff_to_date
	) AS v_eff_to_date,
	v_eff_to_date AS eff_to_date,
	0 AS crrnt_snpsht_flag,
	sysdate AS modified_date,
	eff_from_date AS v_PREV_ROW_EFF_FROM_DATE,
	trans_rsn_code AS v_PREV_ROW_trans_rsn_code
	FROM SQ_sup_transaction_reason
),
FIL_FirstrowinAKgroup AS (
	SELECT
	sup_trans_rsn_id, 
	eff_to_date, 
	crrnt_snpsht_flag, 
	modified_date, 
	orig_eff_to_date
	FROM EXP_Lag_eff_from_date
	WHERE orig_eff_to_date <> eff_to_date
),
UPD_sup_transaction_reason AS (
	SELECT
	sup_trans_rsn_id, 
	eff_to_date, 
	crrnt_snpsht_flag, 
	modified_date
	FROM FIL_FirstrowinAKgroup
),
sup_claims_transaction_reason_update AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_claim_transaction_reason AS T
	USING UPD_sup_transaction_reason AS S
	ON T.sup_claim_trans_rsn_id = S.sup_trans_rsn_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snpsht_flag, T.eff_to_date = S.eff_to_date, T.modified_date = S.modified_date
),