WITH
SQ_sup_insured_type_stage AS (
	SELECT
		sup_insured_type_stage_id,
		insured_type_code,
		insured_type_desc,
		modified_date,
		modified_user_id,
		extract_date,
		as_of_date,
		record_count,
		source_system_id
	FROM sup_insured_type_stage
),
EXP_Default_Values AS (
	SELECT
	insured_type_code,
	-- *INF*: iif(isnull(insured_type_code),'N/A',insured_type_code)
	IFF(insured_type_code IS NULL, 'N/A', insured_type_code) AS insured_type_code_OUT,
	insured_type_desc,
	-- *INF*: iif(isnull(insured_type_desc),'N/A',insured_type_desc)
	IFF(insured_type_desc IS NULL, 'N/A', insured_type_desc) AS insured_type_desc_OUT
	FROM SQ_sup_insured_type_stage
),
LKP_sup_insured_type AS (
	SELECT
	IN_insured_type_code,
	sup_insd_type_id,
	insd_type_code,
	insd_type_descript,
	crrnt_snpsht_flag
	FROM (
		SELECT sup_insured_type.sup_insd_type_id as sup_insd_type_id,
		 LTRIM(RTRIM(sup_insured_type.insd_type_descript)) as insd_type_descript,
		 sup_insured_type.crrnt_snpsht_flag as crrnt_snpsht_flag,
		 LTRIM(RTRIM(sup_insured_type.insd_type_code)) as insd_type_code 
		FROM sup_insured_type where crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY insd_type_code ORDER BY IN_insured_type_code) = 1
),
EXP_Detect_Changes AS (
	SELECT
	LKP_sup_insured_type.sup_insd_type_id AS OLD_sup_insd_type_id,
	LKP_sup_insured_type.insd_type_descript AS OLD_insd_type_descript,
	EXP_Default_Values.insured_type_code_OUT,
	EXP_Default_Values.insured_type_desc_OUT AS employer_type_desc_OUT,
	-- *INF*: IIF(ISNULL(OLD_sup_insd_type_id), 'NEW', IIF(LTRIM(RTRIM(OLD_insd_type_descript)) != (LTRIM(RTRIM(employer_type_desc_OUT))), 'UPDATE', 'NOCHANGE'))
	IFF(OLD_sup_insd_type_id IS NULL, 'NEW', IFF(LTRIM(RTRIM(OLD_insd_type_descript)) != ( LTRIM(RTRIM(employer_type_desc_OUT)) ), 'UPDATE', 'NOCHANGE')) AS V_changed_flag,
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
	LEFT JOIN LKP_sup_insured_type
	ON LKP_sup_insured_type.insd_type_code = EXP_Default_Values.insured_type_code_OUT
),
FIL_sup_insured_type AS (
	SELECT
	insured_type_code_OUT, 
	employer_type_desc_OUT, 
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
sup_insured_type_INSERT AS (
	INSERT INTO sup_insured_type
	(insd_type_code, insd_type_descript, crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date)
	SELECT 
	insured_type_code_OUT AS INSD_TYPE_CODE, 
	employer_type_desc_OUT AS INSD_TYPE_DESCRIPT, 
	CRRNT_SNPSHT_FLAG, 
	AUDIT_ID, 
	EFF_FROM_DATE, 
	EFF_TO_DATE, 
	SOURCE_SYS_ID, 
	CREATED_DATE, 
	MODIFIED_DATE
	FROM FIL_sup_insured_type
),
SQ_sup_insured_type AS (
	SELECT a.sup_insd_type_id,a.insd_type_code, a.eff_from_date, a.eff_to_date 
	FROM
	 @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_insured_type a 
	    WHERE EXISTS ( SELECT 1
	                              FROM  @{pipeline().parameters.TARGET_TABLE_OWNER}. sup_insured_type b
			WHERE source_sys_id= '@{pipeline().parameters.SOURCE_SYSTEM_ID}'  AND crrnt_snpsht_flag = 1
			AND a.insd_type_code = b.insd_type_code            
	 GROUP BY insd_type_code
	             HAVING COUNT(*) > 1)
	ORDER BY insd_type_code  , eff_from_date  DESC
	
	--EXISTS Subquery exists to pick AK Groups that have multiple rows with a 12/31/2100 eff_to_date.
	--When this condition occurs this is an indication that we must expire one or more of these rows.
	--WHERE clause is always made up of current snapshot flag and all columns of the AK
	--GROUP BY clause is always on AK
	--HAVING clause stays the same
	
	--ORDER BY of main query orders all rows first by the AK and then by the eff_from_date in a DESC format
	--the descending order is important because this allows us to avoid another lookup and properly apply the eff_to_date by utilizing a local variable to keep track
),
EXP_Lag_Eff_From_Date AS (
	SELECT
	sup_insd_type_id,
	insd_type_code,
	eff_from_date,
	eff_to_date AS orig_eff_to_date,
	-- *INF*: DECODE(TRUE,
	-- 	insd_type_code= v_prev_row_insd_type_cod, ADD_TO_DATE(v_prev_eff_from_date,'SS',-1),
	-- 	orig_eff_to_date)
	-- 	
	DECODE(TRUE,
		insd_type_code = v_prev_row_insd_type_cod, ADD_TO_DATE(v_prev_eff_from_date, 'SS', - 1),
		orig_eff_to_date) AS v_eff_to_date,
	v_eff_to_date AS eff_to_date,
	insd_type_code AS v_prev_row_insd_type_cod,
	eff_from_date AS v_prev_eff_from_date,
	0 AS crrnt_snpsht_flag,
	SYSDATE AS modified_date
	FROM SQ_sup_insured_type
),
FIL_First_Row_In_AK_Group AS (
	SELECT
	sup_insd_type_id, 
	orig_eff_to_date, 
	eff_to_date, 
	crrnt_snpsht_flag, 
	modified_date
	FROM EXP_Lag_Eff_From_Date
	WHERE orig_eff_to_date !=eff_to_date
),
UPD_sup_insured_type AS (
	SELECT
	sup_insd_type_id, 
	eff_to_date, 
	crrnt_snpsht_flag, 
	modified_date
	FROM FIL_First_Row_In_AK_Group
),
sup_insured_type_UPDATE AS (
	MERGE INTO sup_insured_type AS T
	USING UPD_sup_insured_type AS S
	ON T.sup_insd_type_id = S.sup_insd_type_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snpsht_flag, T.eff_to_date = S.eff_to_date, T.modified_date = S.modified_date
),