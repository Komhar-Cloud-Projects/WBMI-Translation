WITH
SQ_Sup_Occupation_Stage AS (
	SELECT
		sup_occupation_stage_id,
		occupation_code,
		occupation_desc,
		modified_date,
		modified_user_id,
		extract_date,
		as_of_date,
		record_count,
		source_system_id
	FROM Sup_Occupation_Stage
),
EXP_Default_Values AS (
	SELECT
	occupation_code,
	-- *INF*: IIF(ISNULL(occupation_code), 'N/A', occupation_code)
	IFF(occupation_code IS NULL, 'N/A', occupation_code) AS occupation_code_out,
	occupation_desc,
	-- *INF*: IIF(ISNULL(occupation_desc), 'N/A', occupation_desc)
	IFF(occupation_desc IS NULL, 'N/A', occupation_desc) AS occupation_desc_out
	FROM SQ_Sup_Occupation_Stage
),
LKP_sup_workers_comp_occupation AS (
	SELECT
	sup_wc_occuptn_id,
	occuptn_descript,
	occuptn_code
	FROM (
		SELECT sup_workers_comp_occupation.sup_wc_occuptn_id as sup_wc_occuptn_id, 
		sup_workers_comp_occupation.occuptn_descript as occuptn_descript, 
		ltrim(rtrim(sup_workers_comp_occupation.occuptn_code)) as occuptn_code 
		FROM sup_workers_comp_occupation
		where crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY occuptn_code ORDER BY sup_wc_occuptn_id) = 1
),
EXP_detect_changes AS (
	SELECT
	LKP_sup_workers_comp_occupation.sup_wc_occuptn_id AS lkp_sup_wc_occuptn_id,
	LKP_sup_workers_comp_occupation.occuptn_descript AS lkp_occuptn_descript,
	EXP_Default_Values.occupation_code_out AS occupation_code,
	EXP_Default_Values.occupation_desc_out AS occupation_desc,
	-- *INF*: IIF(ISNULL(lkp_sup_wc_occuptn_id), 'NEW', IIF(LTRIM(RTRIM(lkp_occuptn_descript)) != (LTRIM(RTRIM(occupation_desc))), 'UPDATE', 'NOCHANGE'))
	-- 
	IFF(lkp_sup_wc_occuptn_id IS NULL, 'NEW', IFF(LTRIM(RTRIM(lkp_occuptn_descript)) != ( LTRIM(RTRIM(occupation_desc)) ), 'UPDATE', 'NOCHANGE')) AS v_CHANGED_FLAG,
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
	LEFT JOIN LKP_sup_workers_comp_occupation
	ON LKP_sup_workers_comp_occupation.occuptn_code = EXP_Default_Values.occupation_code_out
),
FIL_sup_workers_comp_occupation_insert AS (
	SELECT
	occupation_code AS occupation_code_out, 
	occupation_desc AS occupation_desc_out, 
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
TGT_sup_workers_comp_occupation_INSERT AS (
	INSERT INTO sup_workers_comp_occupation
	(occuptn_code, occuptn_descript, crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date)
	SELECT 
	occupation_code_out AS OCCUPTN_CODE, 
	occupation_desc_out AS OCCUPTN_DESCRIPT, 
	CRRNT_SNPSHT_FLAG, 
	AUDIT_ID, 
	EFF_FROM_DATE, 
	EFF_TO_DATE, 
	SOURCE_SYS_ID, 
	CREATED_DATE, 
	MODIFIED_DATE
	FROM FIL_sup_workers_comp_occupation_insert
),
SQ_sup_workers_comp_occupation AS (
	SELECT a.sup_wc_occuptn_id, a.occuptn_code, a.eff_from_date, a.eff_to_date 
	FROM
	 @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_workers_comp_occupation a
	
	WHERE EXISTS(SELECT 1			
			FROM  @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_workers_comp_occupation b
			WHERE source_sys_id= '@{pipeline().parameters.SOURCE_SYSTEM_ID}'  AND crrnt_snpsht_flag = 1
			AND a.occuptn_code = b.occuptn_code
			GROUP BY occuptn_code
			HAVING COUNT(*) > 1)
	ORDER BY occuptn_code, eff_from_date  DESC
),
EXP_lag_eff_from_date AS (
	SELECT
	sup_wc_occuptn_id,
	occuptn_code,
	eff_from_date,
	eff_to_date AS orig_eff_to_date,
	-- *INF*: DECODE(TRUE,
	-- 	occuptn_code= v_Prev_row_occuptn_code, ADD_TO_DATE(v_prev_eff_from_date,'SS',-1),
	-- 	orig_eff_to_date)
	-- 	
	DECODE(TRUE,
		occuptn_code = v_Prev_row_occuptn_code, ADD_TO_DATE(v_prev_eff_from_date, 'SS', - 1),
		orig_eff_to_date) AS v_eff_to_date,
	v_eff_to_date AS eff_to_date,
	occuptn_code AS v_Prev_row_occuptn_code,
	eff_from_date AS v_prev_eff_from_date,
	0 AS crrnt_snpsht_flag,
	SYSDATE AS modified_date
	FROM SQ_sup_workers_comp_occupation
),
FIL_First_rown_inAKGroup AS (
	SELECT
	sup_wc_occuptn_id, 
	orig_eff_to_date, 
	eff_to_date, 
	crrnt_snpsht_flag, 
	modified_date
	FROM EXP_lag_eff_from_date
	WHERE orig_eff_to_date != eff_to_date
),
UPD_sup_workers_comp_occupation AS (
	SELECT
	sup_wc_occuptn_id, 
	eff_to_date, 
	crrnt_snpsht_flag, 
	modified_date
	FROM FIL_First_rown_inAKGroup
),
TGT_sup_workers_comp_occupation_UPDATE AS (
	MERGE INTO sup_workers_comp_occupation AS T
	USING UPD_sup_workers_comp_occupation AS S
	ON T.sup_wc_occuptn_id = S.sup_wc_occuptn_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snpsht_flag, T.eff_to_date = S.eff_to_date, T.modified_date = S.modified_date
),