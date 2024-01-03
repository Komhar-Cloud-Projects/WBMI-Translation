WITH
SQ_sup_claim_category_stage AS (
	SELECT
		sup_claim_category_stage_id,
		clm_category_code,
		clm_category_desc,
		modified_date,
		modified_user_id,
		extract_date,
		as_of_date,
		record_count,
		source_system_id
	FROM sup_claim_category_stage
),
EXP_Default_Values AS (
	SELECT
	clm_category_code,
	-- *INF*: iif(isnull(clm_category_code),'N/A', LTRIM(RTRIM(clm_category_code)))
	IFF(clm_category_code IS NULL, 'N/A', LTRIM(RTRIM(clm_category_code))) AS clm_category_code_OUT,
	clm_category_desc,
	-- *INF*: iif(isnull(clm_category_desc),'N/A', LTRIM(RTRIM(clm_category_desc)))
	IFF(clm_category_desc IS NULL, 'N/A', LTRIM(RTRIM(clm_category_desc))) AS clm_category_desc_OUT
	FROM SQ_sup_claim_category_stage
),
LKP_SUP_WC_CLAIM_CTGRY AS (
	SELECT
	IN_clm_category_code,
	sup_wc_claim_ctgry_id,
	claim_ctgry_code,
	claim_ctgry_code_descript
	FROM (
		SELECT sup_workers_comp_claim_category.sup_wc_claim_ctgry_id as sup_wc_claim_ctgry_id, 
		LTRIM(RTRIM(sup_workers_comp_claim_category.claim_ctgry_code_descript)) as claim_ctgry_code_descript, 
		LTRIM(RTRIM(sup_workers_comp_claim_category.claim_ctgry_code)) as claim_ctgry_code FROM sup_workers_comp_claim_category
		where crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_ctgry_code ORDER BY IN_clm_category_code) = 1
),
EXP_Detect_Changes AS (
	SELECT
	LKP_SUP_WC_CLAIM_CTGRY.claim_ctgry_code AS OLD_claim_ctgry_code,
	LKP_SUP_WC_CLAIM_CTGRY.claim_ctgry_code_descript AS OLD_claim_ctgry_code_descript,
	EXP_Default_Values.clm_category_code_OUT,
	EXP_Default_Values.clm_category_desc_OUT AS claim_ctgry_code_descript_OUT,
	-- *INF*: IIF(ISNULL(OLD_claim_ctgry_code), 'NEW', IIF(LTRIM(RTRIM(OLD_claim_ctgry_code_descript)) != (LTRIM(RTRIM(claim_ctgry_code_descript_OUT))), 'UPDATE', 'NOCHANGE'))
	IFF(OLD_claim_ctgry_code IS NULL, 'NEW', IFF(LTRIM(RTRIM(OLD_claim_ctgry_code_descript)) != ( LTRIM(RTRIM(claim_ctgry_code_descript_OUT)) ), 'UPDATE', 'NOCHANGE')) AS V_changed_flag,
	V_changed_flag AS CHANGED_FLAG,
	1 AS crrnt_snpsht_flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	-- *INF*: iif(V_changed_flag='NEW',
	-- 	to_date('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'),sysdate)
	IFF(V_changed_flag = 'NEW', to_date('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'), sysdate) AS eff_from_date,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS eff_to_date,
	SYSDATE AS created_date,
	SYSDATE AS modified_date,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS source_sys_id
	FROM EXP_Default_Values
	LEFT JOIN LKP_SUP_WC_CLAIM_CTGRY
	ON LKP_SUP_WC_CLAIM_CTGRY.claim_ctgry_code = EXP_Default_Values.clm_category_code_OUT
),
FIL_sup_workers_comp_employer_type AS (
	SELECT
	clm_category_code_OUT, 
	claim_ctgry_code_descript_OUT, 
	CHANGED_FLAG, 
	crrnt_snpsht_flag, 
	audit_id, 
	eff_from_date, 
	eff_to_date, 
	created_date, 
	modified_date, 
	source_sys_id
	FROM EXP_Detect_Changes
	WHERE CHANGED_FLAG = 'NEW' or CHANGED_FLAG = 'UPDATE'
),
sup_workers_comp_claim_category_insert AS (
	INSERT INTO sup_workers_comp_claim_category
	(claim_ctgry_code, claim_ctgry_code_descript, crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date)
	SELECT 
	clm_category_code_OUT AS CLAIM_CTGRY_CODE, 
	claim_ctgry_code_descript_OUT AS CLAIM_CTGRY_CODE_DESCRIPT, 
	CRRNT_SNPSHT_FLAG, 
	AUDIT_ID, 
	EFF_FROM_DATE, 
	EFF_TO_DATE, 
	SOURCE_SYS_ID, 
	CREATED_DATE, 
	MODIFIED_DATE
	FROM FIL_sup_workers_comp_employer_type
),
SQ_sup_workers_comp_claim_category AS (
	SELECT a.sup_wc_claim_ctgry_id, a.claim_ctgry_code, a.eff_from_date, a.eff_to_date 
	FROM
	  @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_workers_comp_claim_category a 
	WHERE EXISTS(SELECT 1			
			FROM  @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_workers_comp_claim_category b
	WHERE source_sys_id= '@{pipeline().parameters.SOURCE_SYSTEM_ID}'  AND crrnt_snpsht_flag = 1
			AND a.claim_ctgry_code = b.claim_ctgry_code
			GROUP BY claim_ctgry_code
			HAVING COUNT(*) > 1)
	ORDER BY claim_ctgry_code, eff_from_date  DESC
),
EXP_Lag_Eff_From_Date AS (
	SELECT
	sup_wc_claim_ctgry_id AS sup_wc_emplyr_type_id,
	claim_ctgry_code,
	eff_from_date,
	eff_to_date AS orig_eff_to_date,
	-- *INF*: DECODE(TRUE,
	-- 	claim_ctgry_code= v_prev_row_claim_ctgry_code, ADD_TO_DATE(v_prev_eff_from_date,'SS',-1),
	-- 	orig_eff_to_date)
	-- 	
	DECODE(TRUE,
	claim_ctgry_code = v_prev_row_claim_ctgry_code, ADD_TO_DATE(v_prev_eff_from_date, 'SS', - 1),
	orig_eff_to_date) AS v_eff_to_date,
	v_eff_to_date AS eff_to_date,
	claim_ctgry_code AS v_prev_row_claim_ctgry_code,
	eff_from_date AS v_prev_eff_from_date,
	0 AS crrnt_snpsht_flag,
	SYSDATE AS modified_date
	FROM SQ_sup_workers_comp_claim_category
),
FIL_First_Row_In_AK_Group AS (
	SELECT
	sup_wc_emplyr_type_id, 
	orig_eff_to_date, 
	eff_to_date, 
	crrnt_snpsht_flag, 
	modified_date
	FROM EXP_Lag_Eff_From_Date
	WHERE orig_eff_to_date !=eff_to_date
),
UPD_sup_workers_comp_employer_type AS (
	SELECT
	sup_wc_emplyr_type_id, 
	eff_to_date, 
	crrnt_snpsht_flag, 
	modified_date
	FROM FIL_First_Row_In_AK_Group
),
sup_workers_comp_claim_category_update AS (
	MERGE INTO sup_workers_comp_claim_category AS T
	USING UPD_sup_workers_comp_employer_type AS S
	ON T.sup_wc_claim_ctgry_id = S.sup_wc_emplyr_type_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snpsht_flag, T.eff_to_date = S.eff_to_date, T.modified_date = S.modified_date
),