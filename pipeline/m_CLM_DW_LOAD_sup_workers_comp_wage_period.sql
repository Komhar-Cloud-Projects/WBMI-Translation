WITH
SQ_sup_wage_basis_stage AS (
	SELECT
		sup_wage_basis_stage_id,
		wage_basis_code,
		wage_basis_desc,
		modified_date,
		modified_user_id,
		extract_date,
		as_of_date,
		record_count,
		source_system_id
	FROM sup_wage_basis_stage
),
EXP_Default_Values AS (
	SELECT
	wage_basis_code,
	-- *INF*: IIF(ISNULL(wage_basis_code), 'N/A', wage_basis_code)
	IFF(wage_basis_code IS NULL, 'N/A', wage_basis_code) AS wage_basis_code_out,
	wage_basis_desc,
	-- *INF*: IIF(ISNULL(wage_basis_desc), 'N/A', wage_basis_desc)
	IFF(wage_basis_desc IS NULL, 'N/A', wage_basis_desc) AS wage_basis_desc_out
	FROM SQ_sup_wage_basis_stage
),
LKP_sup_workers_comp_wage_period AS (
	SELECT
	sup_wc_wage_period_id,
	wage_period_descript,
	wage_period_code
	FROM (
		SELECT sup_workers_comp_wage_period.sup_wc_wage_period_id as sup_wc_wage_period_id, sup_workers_comp_wage_period.wage_period_descript as wage_period_descript, 
		ltrim(rtrim(sup_workers_comp_wage_period.wage_period_code)) as wage_period_code FROM sup_workers_comp_wage_period
		where crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY wage_period_code ORDER BY sup_wc_wage_period_id) = 1
),
EXP_detect_changes AS (
	SELECT
	LKP_sup_workers_comp_wage_period.sup_wc_wage_period_id AS lkp_sup_wc_wage_period_id,
	LKP_sup_workers_comp_wage_period.wage_period_descript AS lkp_wage_period_descript,
	EXP_Default_Values.wage_basis_code_out AS wage_basis_code,
	EXP_Default_Values.wage_basis_desc_out AS wage_basis_desc,
	-- *INF*: IIF(ISNULL(lkp_sup_wc_wage_period_id), 'NEW', IIF(LTRIM(RTRIM(lkp_wage_period_descript)) != (LTRIM(RTRIM(wage_basis_desc))), 'UPDATE', 'NOCHANGE'))
	-- 
	IFF(
	    lkp_sup_wc_wage_period_id IS NULL, 'NEW',
	    IFF(
	        LTRIM(RTRIM(lkp_wage_period_descript)) != (LTRIM(RTRIM(wage_basis_desc))), 'UPDATE',
	        'NOCHANGE'
	    )
	) AS v_CHANGED_FLAG,
	v_CHANGED_FLAG AS CHANGED_FLAG,
	1 AS crrnt_snpsht_flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	-- *INF*: iif(v_CHANGED_FLAG='NEW',
	-- 	to_date('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'),sysdate)
	IFF(
	    v_CHANGED_FLAG = 'NEW', TO_TIMESTAMP('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'),
	    CURRENT_TIMESTAMP
	) AS eff_from_date,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_TIMESTAMP('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS eff_to_date,
	SYSDATE AS created_date,
	SYSDATE AS modified_date,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS source_sys_id
	FROM EXP_Default_Values
	LEFT JOIN LKP_sup_workers_comp_wage_period
	ON LKP_sup_workers_comp_wage_period.wage_period_code = EXP_Default_Values.wage_basis_code_out
),
FIL_sup_workers_comp_wage_period_INSERT AS (
	SELECT
	wage_basis_code, 
	wage_basis_desc, 
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
TGT_sup_workers_comp_wage_period_INSERT AS (
	INSERT INTO sup_workers_comp_wage_period
	(wage_period_code, wage_period_descript, crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date)
	SELECT 
	wage_basis_code AS WAGE_PERIOD_CODE, 
	wage_basis_desc AS WAGE_PERIOD_DESCRIPT, 
	CRRNT_SNPSHT_FLAG, 
	AUDIT_ID, 
	EFF_FROM_DATE, 
	EFF_TO_DATE, 
	SOURCE_SYS_ID, 
	CREATED_DATE, 
	MODIFIED_DATE
	FROM FIL_sup_workers_comp_wage_period_INSERT
),
SQ_sup_workers_comp_wage_period AS (
	SELECT a.sup_wc_wage_period_id, 
	a.wage_period_code, 
	a.eff_from_date, 
	a.eff_to_date 
	FROM
	 @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_workers_comp_wage_period a
	WHERE EXISTS(SELECT 1			
			FROM  @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_workers_comp_wage_period b
			WHERE source_sys_id= '@{pipeline().parameters.SOURCE_SYSTEM_ID}'  AND crrnt_snpsht_flag = 1
			AND a.wage_period_code = b.wage_period_code
			GROUP BY wage_period_code
			HAVING COUNT(*) > 1)
	ORDER BY wage_period_code, eff_from_date  DESC
),
EXP_lag_eff_from_date AS (
	SELECT
	sup_wc_wage_period_id,
	wage_period_code,
	eff_from_date,
	eff_to_date AS orig_eff_to_date,
	-- *INF*: DECODE(TRUE,
	-- 	wage_period_code= v_Prev_row_occuptn_code, ADD_TO_DATE(v_prev_eff_from_date,'SS',-1),
	-- 	orig_eff_to_date)
	-- 	
	DECODE(
	    TRUE,
	    wage_period_code = v_Prev_row_occuptn_code, DATEADD(SECOND,- 1,v_prev_eff_from_date),
	    orig_eff_to_date
	) AS v_eff_to_date,
	v_eff_to_date AS eff_to_date,
	wage_period_code AS v_Prev_row_occuptn_code,
	eff_from_date AS v_prev_eff_from_date,
	0 AS crrnt_snpsht_flag,
	SYSDATE AS modified_date
	FROM SQ_sup_workers_comp_wage_period
),
FIL_First_rown_inAKGroup AS (
	SELECT
	sup_wc_wage_period_id, 
	orig_eff_to_date, 
	eff_to_date, 
	crrnt_snpsht_flag, 
	modified_date
	FROM EXP_lag_eff_from_date
	WHERE orig_eff_to_date != eff_to_date
),
UPD_sup_workers_comp_wage_period AS (
	SELECT
	sup_wc_wage_period_id, 
	eff_to_date, 
	crrnt_snpsht_flag, 
	modified_date
	FROM FIL_First_rown_inAKGroup
),
TGT_sup_workers_comp_wage_period_UPDATE AS (
	MERGE INTO sup_workers_comp_wage_period AS T
	USING UPD_sup_workers_comp_wage_period AS S
	ON T.sup_wc_wage_period_id = S.sup_wc_wage_period_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snpsht_flag, T.eff_to_date = S.eff_to_date, T.modified_date = S.modified_date
),