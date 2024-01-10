WITH
SQ_sup_activity_stage AS (
	SELECT
		sup_activity_stage_id,
		act_status_code,
		act_status_desc,
		modified_date,
		modified_user_id,
		extract_date,
		as_of_date,
		record_count,
		source_system_id
	FROM sup_activity_stage
),
EXP_Default_Values AS (
	SELECT
	act_status_code,
	-- *INF*: iif(isnull(act_status_code),'N/A',LTRIM(RTRIM(act_status_code)))
	IFF(act_status_code IS NULL,
		'N/A',
		LTRIM(RTRIM(act_status_code
			)
		)
	) AS act_status_code_OUT,
	act_status_desc,
	-- *INF*: iif(isnull(act_status_desc),'N/A',LTRIM(RTRIM(act_status_desc)))
	IFF(act_status_desc IS NULL,
		'N/A',
		LTRIM(RTRIM(act_status_desc
			)
		)
	) AS act_status_desc_OUT
	FROM SQ_sup_activity_stage
),
LKP_SUP_WC_ACTIVITY_STATUS AS (
	SELECT
	act_status_code,
	act_status_code_descript,
	act_status_code_OUT
	FROM (
		SELECT LTRIM(RTRIM(sup_workers_comp_activity_status.act_status_code_descript)) as act_status_code_descript, LTRIM(RTRIM(sup_workers_comp_activity_status.act_status_code)) as act_status_code FROM sup_workers_comp_activity_status
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY act_status_code ORDER BY act_status_code) = 1
),
EXP_Detect_Changes AS (
	SELECT
	LKP_SUP_WC_ACTIVITY_STATUS.act_status_code AS act_status_code_OLD,
	LKP_SUP_WC_ACTIVITY_STATUS.act_status_code_descript AS act_status_code_descript_OLD,
	EXP_Default_Values.act_status_code_OUT AS act_status_code,
	EXP_Default_Values.act_status_desc_OUT AS act_status_desc,
	-- *INF*: IIF(ISNULL(act_status_code_OLD), 'NEW', IIF(LTRIM(RTRIM(act_status_code_descript_OLD)) != (LTRIM(RTRIM(act_status_desc))), 'UPDATE', 'NOCHANGE'))
	IFF(act_status_code_OLD IS NULL,
		'NEW',
		IFF(LTRIM(RTRIM(act_status_code_descript_OLD
				)
			) != ( LTRIM(RTRIM(act_status_desc
					)
				) 
			),
			'UPDATE',
			'NOCHANGE'
		)
	) AS V_changed_flag,
	V_changed_flag AS CHANGED_FLAG,
	1 AS crrnt_snpsht_flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	-- *INF*: iif(V_changed_flag='NEW',
	-- 	to_date('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'),sysdate)
	IFF(V_changed_flag = 'NEW',
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
	LEFT JOIN LKP_SUP_WC_ACTIVITY_STATUS
	ON LKP_SUP_WC_ACTIVITY_STATUS.act_status_code = EXP_Default_Values.act_status_code_OUT
),
FIL_sup_workers_comp_activity_status AS (
	SELECT
	act_status_code, 
	act_status_desc, 
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
sup_workers_comp_activity_status_insert AS (
	INSERT INTO sup_workers_comp_activity_status
	(act_status_code, act_status_code_descript, crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date)
	SELECT 
	ACT_STATUS_CODE, 
	act_status_desc AS ACT_STATUS_CODE_DESCRIPT, 
	CRRNT_SNPSHT_FLAG, 
	AUDIT_ID, 
	EFF_FROM_DATE, 
	EFF_TO_DATE, 
	SOURCE_SYS_ID, 
	CREATED_DATE, 
	MODIFIED_DATE
	FROM FIL_sup_workers_comp_activity_status
),
SQ_sup_workers_comp_activity_status AS (
	SELECT a.sup_wc_act_status_id, a.act_status_code, a.eff_from_date, a.eff_to_date 
	FROM
	  @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_workers_comp_activity_status a
	WHERE EXISTS(SELECT 1			
			FROM  @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_workers_comp_activity_status b
			WHERE source_sys_id= '@{pipeline().parameters.SOURCE_SYSTEM_ID}'  AND crrnt_snpsht_flag = 1
			AND a.act_status_code = b.act_status_code
			GROUP BY act_status_code
			HAVING COUNT(*) > 1)
	ORDER BY act_status_code, eff_from_date  DESC
),
EXP_Lag_Eff_From_Date AS (
	SELECT
	sup_wc_act_status_id,
	act_status_code,
	eff_from_date,
	eff_to_date AS orig_eff_to_date,
	-- *INF*: DECODE(TRUE,
	-- 	act_status_code= v_prev_row_claim_ctgry_code, ADD_TO_DATE(v_prev_eff_from_date,'SS',-1),
	-- 	orig_eff_to_date)
	-- 	
	DECODE(TRUE,
		act_status_code = v_prev_row_claim_ctgry_code, DATEADD(SECOND,- 1,v_prev_eff_from_date),
		orig_eff_to_date
	) AS v_eff_to_date,
	v_eff_to_date AS eff_to_date,
	act_status_code AS v_prev_row_claim_ctgry_code,
	eff_from_date AS v_prev_eff_from_date,
	0 AS crrnt_snpsht_flag,
	SYSDATE AS modified_date
	FROM SQ_sup_workers_comp_activity_status
),
FIL_First_Row_In_AK_Group AS (
	SELECT
	sup_wc_act_status_id, 
	orig_eff_to_date, 
	eff_to_date, 
	crrnt_snpsht_flag, 
	modified_date
	FROM EXP_Lag_Eff_From_Date
	WHERE orig_eff_to_date !=eff_to_date
),
UPD_sup_workers_comp_employer_type AS (
	SELECT
	sup_wc_act_status_id, 
	eff_to_date, 
	crrnt_snpsht_flag, 
	modified_date
	FROM FIL_First_Row_In_AK_Group
),
sup_workers_comp_activity_status_Update AS (
	MERGE INTO sup_workers_comp_activity_status AS T
	USING UPD_sup_workers_comp_employer_type AS S
	ON T.sup_wc_act_status_id = S.sup_wc_act_status_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snpsht_flag, T.eff_to_date = S.eff_to_date, T.modified_date = S.modified_date
),