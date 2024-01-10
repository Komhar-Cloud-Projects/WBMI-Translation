WITH
SQ_gtam_tc26_stage AS (
	SELECT
		tc26_stage_id,
		table_fld,
		key_len,
		location,
		master_company_number,
		line_of_business,
		loss_disability_code,
		data_len,
		loss_disability_code_element,
		loss_disability_literal,
		extract_date,
		as_of_date,
		record_count,
		source_system_id
	FROM gtam_tc26_stage
),
EXP_Default_Values AS (
	SELECT
	loss_disability_code,
	-- *INF*: IIF(ISNULL(loss_disability_code), 'N/A', loss_disability_code)
	IFF(loss_disability_code IS NULL,
		'N/A',
		loss_disability_code
	) AS loss_disability_code_OUT,
	loss_disability_literal,
	-- *INF*: IIF(ISNULL(loss_disability_literal), 'N/A', loss_disability_literal)
	IFF(loss_disability_literal IS NULL,
		'N/A',
		loss_disability_literal
	) AS loss_disability_literal_OUT
	FROM SQ_gtam_tc26_stage
),
LKP_sup_claim_pms_loss_disability AS (
	SELECT
	IN_loss_disability_code,
	sup_claim_pms_loss_disability_id,
	loss_disability_code,
	loss_disability_descript
	FROM (
		SELECT sup_claim_pms_loss_disability.sup_claim_pms_loss_disability_id as sup_claim_pms_loss_disability_id, sup_claim_pms_loss_disability.loss_disability_descript as loss_disability_descript, sup_claim_pms_loss_disability.crrnt_snpsht_flag as crrnt_snpsht_flag, sup_claim_pms_loss_disability.audit_id as audit_id, sup_claim_pms_loss_disability.eff_from_date as eff_from_date, sup_claim_pms_loss_disability.eff_to_date as eff_to_date, sup_claim_pms_loss_disability.source_sys_id as source_sys_id, sup_claim_pms_loss_disability.created_date as created_date, sup_claim_pms_loss_disability.modified_date as modified_date, sup_claim_pms_loss_disability.loss_disability_code as loss_disability_code FROM sup_claim_pms_loss_disability where crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY loss_disability_code ORDER BY IN_loss_disability_code) = 1
),
EXP_detect_changes AS (
	SELECT
	LKP_sup_claim_pms_loss_disability.sup_claim_pms_loss_disability_id AS old_sup_claim_pms_loss_disability_id,
	LKP_sup_claim_pms_loss_disability.loss_disability_descript AS old_loss_disability_descript,
	EXP_Default_Values.loss_disability_code_OUT,
	EXP_Default_Values.loss_disability_literal_OUT,
	-- *INF*: IIF(ISNULL(old_sup_claim_pms_loss_disability_id), 'NEW', IIF(old_loss_disability_descript!= loss_disability_literal_OUT, 'UPDATE', 'NOCHANGE'))
	-- 
	IFF(old_sup_claim_pms_loss_disability_id IS NULL,
		'NEW',
		IFF(old_loss_disability_descript != loss_disability_literal_OUT,
			'UPDATE',
			'NOCHANGE'
		)
	) AS v_CHANGED_FLAG,
	v_CHANGED_FLAG AS CHANGED_FLAG,
	1 AS crrnt_snpsht_flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	-- *INF*: iif(v_CHANGED_FLAG='NEW',
	-- 	to_date('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'),sysdate)
	IFF(v_CHANGED_FLAG = 'NEW',
		to_date('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'
		),
		sysdate
	) AS eff_from_date,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS'
	) AS eff_to_date,
	SYSDATE AS created_date,
	SYSDATE AS modified_date,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS source_sys_id
	FROM EXP_Default_Values
	LEFT JOIN LKP_sup_claim_pms_loss_disability
	ON LKP_sup_claim_pms_loss_disability.loss_disability_code = EXP_Default_Values.loss_disability_code_OUT
),
FIL_sup_insurance_line_insert AS (
	SELECT
	loss_disability_code_OUT, 
	loss_disability_literal_OUT, 
	CHANGED_FLAG, 
	crrnt_snpsht_flag, 
	audit_id, 
	eff_from_date, 
	eff_to_date, 
	created_date, 
	modified_date, 
	source_sys_id
	FROM EXP_detect_changes
	WHERE CHANGED_FLAG = 'NEW' or CHANGED_FLAG = 'UPDATE'
),
sup_claim_pms_loss_disability_insert AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_claim_pms_loss_disability
	(loss_disability_code, loss_disability_descript, crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date)
	SELECT 
	loss_disability_code_OUT AS LOSS_DISABILITY_CODE, 
	loss_disability_literal_OUT AS LOSS_DISABILITY_DESCRIPT, 
	CRRNT_SNPSHT_FLAG, 
	AUDIT_ID, 
	EFF_FROM_DATE, 
	EFF_TO_DATE, 
	SOURCE_SYS_ID, 
	CREATED_DATE, 
	MODIFIED_DATE
	FROM FIL_sup_insurance_line_insert
),
SQ_sup_claim_pms_loss_disability AS (
	SELECT a.sup_claim_pms_loss_disability_id, a.loss_disability_code, a.eff_from_date, a.eff_to_date 
	FROM
	 @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_claim_pms_loss_disability a
	
	WHERE EXISTS(SELECT 1			
			FROM  @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_claim_pms_loss_disability  b
			WHERE source_sys_id= '@{pipeline().parameters.SOURCE_SYSTEM_ID}'  AND crrnt_snpsht_flag = 1
			AND a.loss_disability_code = b.loss_disability_code 
			GROUP BY loss_disability_code 
			HAVING COUNT(*) > 1)
	ORDER BY loss_disability_code , eff_from_date  DESC
	
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
	sup_claim_pms_loss_disability_id,
	loss_disability_code,
	eff_from_date,
	eff_to_date AS orig_eff_to_date,
	-- *INF*: DECODE(TRUE,
	-- 	loss_disability_code = v_Prev_row_loss_disability_code, ADD_TO_DATE(v_prev_eff_from_date,'SS',-1),
	-- 	orig_eff_to_date)
	-- 	
	DECODE(TRUE,
		loss_disability_code = v_Prev_row_loss_disability_code, DATEADD(SECOND,- 1,v_prev_eff_from_date),
		orig_eff_to_date
	) AS v_eff_to_date,
	v_eff_to_date AS eff_to_date,
	loss_disability_code AS v_Prev_row_loss_disability_code,
	eff_from_date AS v_prev_eff_from_date,
	0 AS crrnt_snpsht_flag,
	SYSDATE AS modified_date
	FROM SQ_sup_claim_pms_loss_disability
),
FIL_First_rown_inAKGroup AS (
	SELECT
	orig_eff_to_date, 
	eff_to_date, 
	crrnt_snpsht_flag, 
	modified_date
	FROM EXP_lag_eff_from_date
	WHERE orig_eff_to_date != eff_to_date
),
UPD_sup_insurance_line AS (
	SELECT
	sup_risk_unit_grp_id, 
	eff_to_date, 
	crrnt_snpsht_flag, 
	modified_date
	FROM FIL_First_rown_inAKGroup
),
sup_claim_pms_loss_disability_update AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_claim_pms_loss_disability AS T
	USING UPD_sup_insurance_line AS S
	ON T.sup_claim_pms_loss_disability_id = S.sup_risk_unit_grp_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snpsht_flag, T.eff_to_date = S.eff_to_date, T.modified_date = S.modified_date
),