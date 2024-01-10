WITH
SQ_sup_wc_emplymnt_st_stage AS (
	SELECT
		sup_employmnt_st_stage_id,
		wc_code,
		wc_description,
		modified_date,
		modified_user_id,
		extract_date,
		as_of_date,
		record_count,
		source_system_id
	FROM sup_wc_emplymnt_st_stage
),
EXP_Default_Values AS (
	SELECT
	wc_code,
	-- *INF*: IIF(ISNULL(wc_code), 'N/A', wc_code)
	IFF(wc_code IS NULL,
		'N/A',
		wc_code
	) AS wc_code_out,
	wc_description,
	-- *INF*: IIF(ISNULL(wc_description), 'N/A', wc_description)
	IFF(wc_description IS NULL,
		'N/A',
		wc_description
	) AS wc_description_out
	FROM SQ_sup_wc_emplymnt_st_stage
),
LKP_sup_workers_comp_employment_status AS (
	SELECT
	sup_wc_emplymnt_status_id,
	wc_emplymnt_descript,
	wc_emplymnt_code
	FROM (
		SELECT sup_workers_comp_employment_status.sup_wc_emplymnt_status_id as sup_wc_emplymnt_status_id, sup_workers_comp_employment_status.wc_emplymnt_descript as wc_emplymnt_descript,
		rtrim(ltrim(sup_workers_comp_employment_status.wc_emplymnt_code)) as wc_emplymnt_code 
		FROM sup_workers_comp_employment_status
		where crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY wc_emplymnt_code ORDER BY sup_wc_emplymnt_status_id) = 1
),
EXP_detect_changes AS (
	SELECT
	LKP_sup_workers_comp_employment_status.sup_wc_emplymnt_status_id AS lkp_sup_wc_emplymnt_status_id,
	LKP_sup_workers_comp_employment_status.wc_emplymnt_descript AS lkp_wc_emplymnt_descript,
	EXP_Default_Values.wc_code_out AS wc_code,
	EXP_Default_Values.wc_description_out AS wc_description,
	-- *INF*: IIF(ISNULL(lkp_sup_wc_emplymnt_status_id), 'NEW', IIF(LTRIM(RTRIM(lkp_wc_emplymnt_descript)) != (LTRIM(RTRIM(wc_description))), 'UPDATE', 'NOCHANGE'))
	-- 
	IFF(lkp_sup_wc_emplymnt_status_id IS NULL,
		'NEW',
		IFF(LTRIM(RTRIM(lkp_wc_emplymnt_descript
				)
			) != ( LTRIM(RTRIM(wc_description
					)
				) 
			),
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
	LEFT JOIN LKP_sup_workers_comp_employment_status
	ON LKP_sup_workers_comp_employment_status.wc_emplymnt_code = EXP_Default_Values.wc_code_out
),
FIL_sup_workers_comp_employment_status_INSERT AS (
	SELECT
	wc_code, 
	wc_description, 
	CHANGED_FLAG, 
	crrnt_snpsht_flag, 
	audit_id, 
	eff_from_date, 
	eff_to_date, 
	source_sys_id, 
	created_date, 
	modified_date
	FROM EXP_detect_changes
	WHERE CHANGED_FLAG = 'NEW' or CHANGED_FLAG = 'UPDATE'
),
TGT_sup_workers_comp_employment_status_INSERT AS (
	INSERT INTO sup_workers_comp_employment_status
	(wc_emplymnt_code, wc_emplymnt_descript, crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date)
	SELECT 
	wc_code AS WC_EMPLYMNT_CODE, 
	wc_description AS WC_EMPLYMNT_DESCRIPT, 
	CRRNT_SNPSHT_FLAG, 
	AUDIT_ID, 
	EFF_FROM_DATE, 
	EFF_TO_DATE, 
	SOURCE_SYS_ID, 
	CREATED_DATE, 
	MODIFIED_DATE
	FROM FIL_sup_workers_comp_employment_status_INSERT
),
SQ_sup_workers_comp_employment_status AS (
	SELECT a.sup_wc_emplymnt_status_id, a.wc_emplymnt_code, a.eff_from_date, a.eff_to_date 
	FROM
	 @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_workers_comp_employment_status a
	
	WHERE EXISTS(SELECT 1			
			FROM  @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_workers_comp_employment_status b
			WHERE source_sys_id= '@{pipeline().parameters.SOURCE_SYSTEM_ID}'  AND crrnt_snpsht_flag = 1
			AND a.wc_emplymnt_code = b.wc_emplymnt_code
			GROUP BY wc_emplymnt_code
			HAVING COUNT(*) > 1)
	ORDER BY wc_emplymnt_code, eff_from_date  DESC
),
EXP_lag_eff_from_date AS (
	SELECT
	sup_wc_emplymnt_status_id,
	wc_emplymnt_code,
	eff_from_date,
	eff_to_date AS orig_eff_to_date,
	-- *INF*: DECODE(TRUE,
	-- 	wc_emplymnt_code= v_Prev_row_occuptn_code, ADD_TO_DATE(v_prev_eff_from_date,'SS',-1),
	-- 	orig_eff_to_date)
	-- 	
	DECODE(TRUE,
		wc_emplymnt_code = v_Prev_row_occuptn_code, DATEADD(SECOND,- 1,v_prev_eff_from_date),
		orig_eff_to_date
	) AS v_eff_to_date,
	v_eff_to_date AS eff_to_date,
	wc_emplymnt_code AS v_Prev_row_occuptn_code,
	eff_from_date AS v_prev_eff_from_date,
	0 AS crrnt_snpsht_flag,
	SYSDATE AS modified_date
	FROM SQ_sup_workers_comp_employment_status
),
FIL_First_rown_inAKGroup AS (
	SELECT
	sup_wc_emplymnt_status_id, 
	orig_eff_to_date, 
	eff_to_date, 
	crrnt_snpsht_flag, 
	modified_date
	FROM EXP_lag_eff_from_date
	WHERE orig_eff_to_date != eff_to_date
),
UPD_sup_workers_comp_employment_status AS (
	SELECT
	sup_wc_emplymnt_status_id, 
	eff_to_date, 
	crrnt_snpsht_flag, 
	modified_date
	FROM FIL_First_rown_inAKGroup
),
TGT_sup_workers_comp_employment_status_UPDATE AS (
	MERGE INTO sup_workers_comp_employment_status AS T
	USING UPD_sup_workers_comp_employment_status AS S
	ON T.sup_wc_emplymnt_status_id = S.sup_wc_emplymnt_status_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snpsht_flag, T.eff_to_date = S.eff_to_date, T.modified_date = S.modified_date
),