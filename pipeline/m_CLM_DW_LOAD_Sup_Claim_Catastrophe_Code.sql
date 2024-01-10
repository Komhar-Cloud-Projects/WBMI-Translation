WITH
SQ_loss_occurrence_S3P_STAGE AS (
	SELECT
		loss_occurrence_S3P_ID,
		COC_OCCURRENCE_ID,
		COC_ENTRY_OPR_ID,
		COC_OCCURRENCE_TYP,
		COC_OCC_LOC_ID,
		COC_OCC_DES_ID,
		COC_OCC_CMT_ID,
		COC_COUNTRY_CD,
		COC_START_DT,
		COC_END_DT,
		COC_PMSD_TS,
		COC_UPDATE_OPR_ID,
		COC_CREATE_TS,
		COC_UPD_TS,
		COC_CATASTROPHE_CD,
		COC_CSR_OCC_NBR,
		EXTRACT_DATE,
		AS_OF_DATE,
		RECORD_COUNT,
		SOURCE_SYSTEM_ID
	FROM loss_occurrence_S3P_STAGE
),
EXP_Values AS (
	SELECT
	COC_CATASTROPHE_CD,
	COC_START_DT,
	COC_END_DT
	FROM SQ_loss_occurrence_S3P_STAGE
),
EXP_Lkp_Values AS (
	SELECT
	COC_CATASTROPHE_CD AS in_COC_CATASTROPHE_CD,
	-- *INF*: IIF(ISNULL(in_COC_CATASTROPHE_CD), 
	-- 'N/A',
	-- LPAD(rtrim(in_COC_CATASTROPHE_CD),3,'0'))
	IFF(in_COC_CATASTROPHE_CD IS NULL,
		'N/A',
		LPAD(rtrim(in_COC_CATASTROPHE_CD
			), 3, '0'
		)
	) AS COC_CATASTROPHE_CODE,
	COC_START_DT AS in_COC_START_DT,
	-- *INF*: IIF(ISNULL(in_COC_START_DT), 
	-- TO_DATE('1/1/1800','MM/DD/YYYY'),
	-- in_COC_START_DT)
	IFF(in_COC_START_DT IS NULL,
		TO_DATE('1/1/1800', 'MM/DD/YYYY'
		),
		in_COC_START_DT
	) AS COC_START_DT,
	COC_END_DT AS in_COC_END_DT,
	-- *INF*: IIF(ISNULL(in_COC_END_DT),
	-- TO_DATE('12/31/2100','MM/DD/YYYY'), 
	-- in_COC_END_DT)
	IFF(in_COC_END_DT IS NULL,
		TO_DATE('12/31/2100', 'MM/DD/YYYY'
		),
		in_COC_END_DT
	) AS COC_END_DT
	FROM EXP_Values
),
LKP_sup_Claim_catastrophe_code AS (
	SELECT
	sup_claim_cat_code_id,
	cat_start_date,
	cat_end_date,
	cat_code
	FROM (
		SELECT 
		a.sup_claim_cat_code_id as sup_claim_cat_code_id,
		a.cat_start_date as cat_start_date, 
		a.cat_end_date as cat_end_date, 
		rtrim(a.cat_code) as cat_code 
		FROM 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.sup_claim_catastrophe_code a
		WHERE 
		source_system_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'  AND crrnt_snpsht_flag = 1
		ORDER BY cat_code --
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY cat_code ORDER BY sup_claim_cat_code_id) = 1
),
EXP_Detect_Changes AS (
	SELECT
	EXP_Lkp_Values.COC_CATASTROPHE_CODE AS CLM_CATASTROPHE_CODE,
	EXP_Lkp_Values.COC_START_DT,
	EXP_Lkp_Values.COC_END_DT,
	LKP_sup_Claim_catastrophe_code.sup_claim_cat_code_id AS lkp_sup_claim_cat_code_id,
	LKP_sup_Claim_catastrophe_code.cat_start_date AS lkp_cat_start_date,
	LKP_sup_Claim_catastrophe_code.cat_end_date AS lkp_cat_end_date,
	1 AS Crrnt_Snpsht_Flag,
	-- *INF*: IIF(ISNULL(lkp_sup_claim_cat_code_id),'NEW',
	-- 	IIF((
	-- 	COC_START_DT <> lkp_cat_start_date or  
	-- 	COC_END_DT <> lkp_cat_end_date),
	-- 	'UPDATE',
	-- 	'NOCHANGE'))
	IFF(lkp_sup_claim_cat_code_id IS NULL,
		'NEW',
		IFF(( COC_START_DT <> lkp_cat_start_date 
				OR COC_END_DT <> lkp_cat_end_date 
			),
			'UPDATE',
			'NOCHANGE'
		)
	) AS v_Changed_Flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS Audit_Id,
	-- *INF*: IIF(v_Changed_Flag='NEW',
	-- 	TO_DATE('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'),
	-- 	SYSDATE)
	IFF(v_Changed_Flag = 'NEW',
		TO_DATE('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'
		),
		SYSDATE
	) AS Eff_From_Date,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS'
	) AS Eff_To_Date,
	v_Changed_Flag AS Changed_Flag,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS SOURCE_SYSTEM_ID,
	SYSDATE AS Created_Date,
	SYSDATE AS Modified_Date
	FROM EXP_Lkp_Values
	LEFT JOIN LKP_sup_Claim_catastrophe_code
	ON LKP_sup_Claim_catastrophe_code.cat_code = EXP_Lkp_Values.COC_CATASTROPHE_CODE
),
FIL_Insert AS (
	SELECT
	CLM_CATASTROPHE_CODE, 
	COC_START_DT, 
	COC_END_DT, 
	Crrnt_Snpsht_Flag, 
	Audit_Id, 
	Eff_From_Date, 
	Eff_To_Date, 
	Changed_Flag, 
	SOURCE_SYSTEM_ID, 
	Created_Date, 
	Modified_Date
	FROM EXP_Detect_Changes
	WHERE Changed_Flag='NEW'  OR Changed_Flag='UPDATE'
),
sup_claim_catastrophe_code_Insert AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_claim_catastrophe_code
	(cat_code, cat_start_date, cat_end_date, crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_system_id, created_date, modified_date)
	SELECT 
	CLM_CATASTROPHE_CODE AS CAT_CODE, 
	COC_START_DT AS CAT_START_DATE, 
	COC_END_DT AS CAT_END_DATE, 
	Crrnt_Snpsht_Flag AS CRRNT_SNPSHT_FLAG, 
	Audit_Id AS AUDIT_ID, 
	Eff_From_Date AS EFF_FROM_DATE, 
	Eff_To_Date AS EFF_TO_DATE, 
	SOURCE_SYSTEM_ID AS SOURCE_SYSTEM_ID, 
	Created_Date AS CREATED_DATE, 
	Modified_Date AS MODIFIED_DATE
	FROM FIL_Insert
),
SQ_sup_claim_catastrophe_code AS (
	SELECT 
	a.sup_claim_cat_code_id, 
	a.cat_code, 
	a.eff_from_date, 
	a.eff_to_date 
	FROM
	 @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_claim_catastrophe_code a
	WHERE EXISTS(SELECT 1			
			FROM  @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_claim_catastrophe_code b
			WHERE source_system_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'  AND crrnt_snpsht_flag = 1
			AND a.cat_code = b.cat_code
			GROUP BY cat_code
			HAVING COUNT(*) > 1)
	ORDER BY cat_code, eff_from_date  DESC
	
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
	sup_claim_cat_code_id,
	cat_code,
	eff_from_date,
	eff_to_date AS orig_eff_to_date,
	-- *INF*: DECODE(TRUE,
	-- 	cat_code = v_PREV_ROW_cat_key, ADD_TO_DATE(v_PREV_ROW_eff_from_date,'SS',-1),
	-- 	orig_eff_to_date)
	DECODE(TRUE,
		cat_code = v_PREV_ROW_cat_key, DATEADD(SECOND,- 1,v_PREV_ROW_eff_from_date),
		orig_eff_to_date
	) AS v_eff_to_date,
	v_eff_to_date AS eff_to_date,
	eff_from_date AS v_PREV_ROW_eff_from_date,
	cat_code AS v_PREV_ROW_cat_key,
	SYSDATE AS modified_date,
	0 AS crrnt_snpsht_flag
	FROM SQ_sup_claim_catastrophe_code
),
FIL_FirstRowInAKGroup AS (
	SELECT
	sup_claim_cat_code_id, 
	orig_eff_to_date, 
	eff_to_date, 
	modified_date, 
	crrnt_snpsht_flag
	FROM EXP_Lag_eff_from_date
	WHERE orig_eff_to_date <> eff_to_date

--If these two dates equal each other we are dealing with the first row in an AK group.  This row
--does not need to be expired or updated for any reason thus it can be filtered out
-- but we must source it to capture the eff_from_date of this row 
--so that we can properly expire the subsequent row
),
UPD_Sup_Catastrope_Code AS (
	SELECT
	sup_claim_cat_code_id, 
	eff_to_date, 
	modified_date, 
	crrnt_snpsht_flag
	FROM FIL_FirstRowInAKGroup
),
sup_claim_catastrophe_code_Update AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_claim_catastrophe_code AS T
	USING UPD_Sup_Catastrope_Code AS S
	ON T.sup_claim_cat_code_id = S.sup_claim_cat_code_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snpsht_flag, T.eff_to_date = S.eff_to_date, T.modified_date = S.modified_date
),