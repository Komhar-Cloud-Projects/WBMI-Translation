WITH
SQ_Gtam_TC08_stage AS (
	SELECT
		tc08_stage_id,
		table_fld,
		key_len,
		code_entered_on_pucl,
		data_len,
		payee_phrase_verbiage,
		extract_date,
		as_of_date,
		record_count,
		source_system_id
	FROM Gtam_TC08_stage
),
EXP_default AS (
	SELECT
	code_entered_on_pucl,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(code_entered_on_pucl)))OR IS_SPACES(LTRIM(RTRIM(code_entered_on_pucl))) OR LENGTH(LTRIM(RTRIM(code_entered_on_pucl))) =0, 'N/A',LTRIM(RTRIM(code_entered_on_pucl)))
	IFF(
	    LTRIM(RTRIM(code_entered_on_pucl)) IS NULL
	    or LENGTH(LTRIM(RTRIM(code_entered_on_pucl)))>0
	    and TRIM(LTRIM(RTRIM(code_entered_on_pucl)))=''
	    or LENGTH(LTRIM(RTRIM(code_entered_on_pucl))) = 0,
	    'N/A',
	    LTRIM(RTRIM(code_entered_on_pucl))
	) AS code_entered_on_pucl_out,
	payee_phrase_verbiage,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(payee_phrase_verbiage))) OR IS_SPACES(LTRIM(RTRIM(payee_phrase_verbiage))) OR LENGTH(LTRIM(RTRIM(payee_phrase_verbiage))) = 0 ,'N/A' , LTRIM(RTRIM(payee_phrase_verbiage)))
	IFF(
	    LTRIM(RTRIM(payee_phrase_verbiage)) IS NULL
	    or LENGTH(LTRIM(RTRIM(payee_phrase_verbiage)))>0
	    and TRIM(LTRIM(RTRIM(payee_phrase_verbiage)))=''
	    or LENGTH(LTRIM(RTRIM(payee_phrase_verbiage))) = 0,
	    'N/A',
	    LTRIM(RTRIM(payee_phrase_verbiage))
	) AS payee_phrase_verbiage_out
	FROM SQ_Gtam_TC08_stage
),
LKP_Claim_Payee_phrase AS (
	SELECT
	sup_claim_payee_phrase_id,
	payee_phrase_descript,
	payee_phrase_code
	FROM (
		SELECT sup_claim_payee_phrase.sup_claim_payee_phrase_id as sup_claim_payee_phrase_id, sup_claim_payee_phrase.payee_phrase_descript as payee_phrase_descript, sup_claim_payee_phrase.payee_phrase_code as payee_phrase_code 
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_claim_payee_phrase
		WHERE crrnt_snpsht_flag =1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY payee_phrase_code ORDER BY sup_claim_payee_phrase_id) = 1
),
EXP_detect_changes AS (
	SELECT
	LKP_Claim_Payee_phrase.sup_claim_payee_phrase_id,
	LKP_Claim_Payee_phrase.payee_phrase_descript AS old_payee_phrase_comment,
	EXP_default.code_entered_on_pucl_out,
	EXP_default.payee_phrase_verbiage_out,
	-- *INF*: IIF(ISNULL(sup_claim_payee_phrase_id), 'NEW', 
	-- IIF(LTRIM(RTRIM(old_payee_phrase_comment)) != (LTRIM(RTRIM(payee_phrase_verbiage_out))), 'UPDATE', 'NOCHANGE'))
	IFF(
	    sup_claim_payee_phrase_id IS NULL, 'NEW',
	    IFF(
	        LTRIM(RTRIM(old_payee_phrase_comment)) != (LTRIM(RTRIM(payee_phrase_verbiage_out))),
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
	LEFT JOIN LKP_Claim_Payee_phrase
	ON LKP_Claim_Payee_phrase.payee_phrase_code = EXP_default.code_entered_on_pucl_out
),
FIL_new_update AS (
	SELECT
	code_entered_on_pucl_out, 
	payee_phrase_verbiage_out, 
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
sup_claim_payee_phrase_insert AS (
	INSERT INTO sup_claim_payee_phrase
	(payee_phrase_code, payee_phrase_descript, crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date)
	SELECT 
	code_entered_on_pucl_out AS PAYEE_PHRASE_CODE, 
	payee_phrase_verbiage_out AS PAYEE_PHRASE_DESCRIPT, 
	CRRNT_SNPSHT_FLAG, 
	AUDIT_ID, 
	EFF_FROM_DATE, 
	EFF_TO_DATE, 
	SOURCE_SYS_ID, 
	CREATED_DATE, 
	MODIFIED_DATE
	FROM FIL_new_update
),
SQ_sup_claim_payee_phrase AS (
	SELECT a.sup_claim_payee_phrase_id, a.payee_phrase_code, a.eff_from_date, a.eff_to_date 
	FROM
	 @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_claim_payee_phrase a
	
	WHERE EXISTS(SELECT 1			
			FROM  @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_claim_payee_phrase b
			WHERE source_sys_id= '@{pipeline().parameters.SOURCE_SYSTEM_ID}'  AND crrnt_snpsht_flag = 1
			AND a.payee_phrase_code = a.payee_phrase_code
			GROUP BY payee_phrase_code
			HAVING COUNT(*) > 1)
	ORDER BY payee_phrase_code, eff_from_date  DESC
	
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
	sup_claim_payee_phrase_id,
	payee_phrase_code,
	eff_from_date,
	eff_to_date AS orig_eff_to_date,
	-- *INF*: DECODE(TRUE,
	-- 	payee_phrase_code= v_Prev_row_payee_phrase_code, ADD_TO_DATE(v_prev_eff_from_date,'SS',-1),
	-- 	orig_eff_to_date)
	-- 	
	DECODE(
	    TRUE,
	    payee_phrase_code = v_Prev_row_payee_phrase_code, DATEADD(SECOND,- 1,v_prev_eff_from_date),
	    orig_eff_to_date
	) AS v_eff_to_date,
	v_eff_to_date AS eff_to_date,
	payee_phrase_code AS v_Prev_row_payee_phrase_code,
	eff_from_date AS v_prev_eff_from_date,
	0 AS crrnt_snpsht_flag,
	SYSDATE AS modified_date
	FROM SQ_sup_claim_payee_phrase
),
FIL_First_rown_inAKGroup AS (
	SELECT
	sup_claim_payee_phrase_id, 
	orig_eff_to_date, 
	eff_to_date, 
	crrnt_snpsht_flag, 
	modified_date
	FROM EXP_lag_eff_from_date
	WHERE orig_eff_to_date != eff_to_date
),
UPD_eff_from_date AS (
	SELECT
	sup_claim_payee_phrase_id, 
	eff_to_date, 
	crrnt_snpsht_flag, 
	modified_date
	FROM FIL_First_rown_inAKGroup
),
sup_claim_payee_phrase_update AS (
	MERGE INTO sup_claim_payee_phrase AS T
	USING UPD_eff_from_date AS S
	ON T.sup_claim_payee_phrase_id = S.sup_claim_payee_phrase_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snpsht_flag, T.eff_to_date = S.eff_to_date, T.modified_date = S.modified_date
),