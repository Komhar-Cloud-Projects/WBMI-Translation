WITH
SQ_gtam_TC09_stage AS (
	SELECT
		tc09_stage_id,
		table_fld,
		key_len,
		memo_phrase_on_pucl,
		data_len,
		memo_phrase_verbiage,
		extract_date,
		as_of_date,
		record_count,
		source_system_id
	FROM gtam_TC09_stage
),
EXP_default AS (
	SELECT
	memo_phrase_on_pucl,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(memo_phrase_on_pucl)))OR IS_SPACES(LTRIM(RTRIM(memo_phrase_on_pucl))) OR LENGTH(LTRIM(RTRIM(memo_phrase_on_pucl))) =0, 'N/A',LTRIM(RTRIM(memo_phrase_on_pucl)))
	IFF(
	    LTRIM(RTRIM(memo_phrase_on_pucl)) IS NULL
	    or LENGTH(LTRIM(RTRIM(memo_phrase_on_pucl)))>0
	    and TRIM(LTRIM(RTRIM(memo_phrase_on_pucl)))=''
	    or LENGTH(LTRIM(RTRIM(memo_phrase_on_pucl))) = 0,
	    'N/A',
	    LTRIM(RTRIM(memo_phrase_on_pucl))
	) AS memo_phrase_on_pucl_out,
	memo_phrase_verbiage,
	-- *INF*: IIF(ISNULL(LTRIM(RTRIM(memo_phrase_verbiage))) OR IS_SPACES(LTRIM(RTRIM(memo_phrase_verbiage))) OR LENGTH(LTRIM(RTRIM(memo_phrase_verbiage))) = 0 ,'N/A' , LTRIM(RTRIM(memo_phrase_verbiage)))
	IFF(
	    LTRIM(RTRIM(memo_phrase_verbiage)) IS NULL
	    or LENGTH(LTRIM(RTRIM(memo_phrase_verbiage)))>0
	    and TRIM(LTRIM(RTRIM(memo_phrase_verbiage)))=''
	    or LENGTH(LTRIM(RTRIM(memo_phrase_verbiage))) = 0,
	    'N/A',
	    LTRIM(RTRIM(memo_phrase_verbiage))
	) AS memo_phrase_verbiage_out
	FROM SQ_gtam_TC09_stage
),
LKP_Claim_memo_pharse AS (
	SELECT
	sup_memo_phrase_code_id,
	memo_phrase_descript,
	memo_phrase_code
	FROM (
		SELECT 
		sup_claim_memo_phrase.sup_memo_phrase_code_id as sup_memo_phrase_code_id, sup_claim_memo_phrase.memo_phrase_descript as memo_phrase_descript, sup_claim_memo_phrase.memo_phrase_code as memo_phrase_code 
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_claim_memo_phrase
		WHERE crrnt_snpsht_flag =1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY memo_phrase_code ORDER BY sup_memo_phrase_code_id) = 1
),
EXP_detect_changes AS (
	SELECT
	LKP_Claim_memo_pharse.sup_memo_phrase_code_id,
	LKP_Claim_memo_pharse.memo_phrase_descript AS memo_phrase_comment,
	EXP_default.memo_phrase_on_pucl_out,
	EXP_default.memo_phrase_verbiage_out,
	-- *INF*: IIF(ISNULL(sup_memo_phrase_code_id), 'NEW', 
	-- IIF(LTRIM(RTRIM(memo_phrase_comment)) != (LTRIM(RTRIM(memo_phrase_verbiage_out))), 'UPDATE', 'NOCHANGE'))
	IFF(
	    sup_memo_phrase_code_id IS NULL, 'NEW',
	    IFF(
	        LTRIM(RTRIM(memo_phrase_comment)) != (LTRIM(RTRIM(memo_phrase_verbiage_out))),
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
	LEFT JOIN LKP_Claim_memo_pharse
	ON LKP_Claim_memo_pharse.memo_phrase_code = EXP_default.memo_phrase_on_pucl_out
),
FIL_new_update AS (
	SELECT
	memo_phrase_on_pucl_out, 
	memo_phrase_verbiage_out, 
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
sup_claim_memo_phrase_insert AS (
	INSERT INTO sup_claim_memo_phrase
	(memo_phrase_code, memo_phrase_descript, crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date)
	SELECT 
	memo_phrase_on_pucl_out AS MEMO_PHRASE_CODE, 
	memo_phrase_verbiage_out AS MEMO_PHRASE_DESCRIPT, 
	CRRNT_SNPSHT_FLAG, 
	AUDIT_ID, 
	EFF_FROM_DATE, 
	EFF_TO_DATE, 
	SOURCE_SYS_ID, 
	CREATED_DATE, 
	MODIFIED_DATE
	FROM FIL_new_update
),
SQ_sup_claim_memo_phrase AS (
	SELECT a.sup_memo_phrase_code_id, a.memo_phrase_code, a.eff_from_date, a.eff_to_date 
	FROM
	 @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_claim_memo_phrase a
	
	WHERE EXISTS(SELECT 1			
			FROM  @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_claim_memo_phrase b
			WHERE source_sys_id= '@{pipeline().parameters.SOURCE_SYSTEM_ID}'  AND crrnt_snpsht_flag = 1
			AND a.memo_phrase_code = a.memo_phrase_code
			GROUP BY memo_phrase_code
			HAVING COUNT(*) > 1)
	ORDER BY memo_phrase_code, eff_from_date  DESC
	
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
	sup_memo_phrase_code_id,
	memo_phrase_code,
	eff_from_date,
	eff_to_date AS orig_eff_to_date,
	-- *INF*: DECODE(TRUE,
	-- 	memo_phrase_code= v_Prev_row_memo_phrase_code, ADD_TO_DATE(v_prev_eff_from_date,'SS',-1),
	-- 	orig_eff_to_date)
	-- 	
	DECODE(
	    TRUE,
	    memo_phrase_code = v_Prev_row_memo_phrase_code, DATEADD(SECOND,- 1,v_prev_eff_from_date),
	    orig_eff_to_date
	) AS v_eff_to_date,
	v_eff_to_date AS eff_to_date,
	memo_phrase_code AS v_Prev_row_memo_phrase_code,
	eff_from_date AS v_prev_eff_from_date,
	0 AS crrnt_snpsht_flag,
	SYSDATE AS modified_date
	FROM SQ_sup_claim_memo_phrase
),
FIL_First_rown_inAKGroup AS (
	SELECT
	sup_memo_phrase_code_id, 
	orig_eff_to_date, 
	eff_to_date, 
	crrnt_snpsht_flag, 
	modified_date
	FROM EXP_lag_eff_from_date
	WHERE orig_eff_to_date != eff_to_date
),
UPD_eff_from_date AS (
	SELECT
	sup_memo_phrase_code_id, 
	eff_to_date, 
	crrnt_snpsht_flag, 
	modified_date
	FROM FIL_First_rown_inAKGroup
),
sup_claim_memo_phrase_update_crrnt_snpsht_flag AS (
	MERGE INTO sup_claim_memo_phrase AS T
	USING UPD_eff_from_date AS S
	ON T.sup_memo_phrase_code_id = S.sup_memo_phrase_code_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snpsht_flag, T.eff_to_date = S.eff_to_date, T.modified_date = S.modified_date
),