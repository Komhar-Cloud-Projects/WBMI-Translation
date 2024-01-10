WITH
SQ_sup_exemption_type_stage AS (
	SELECT
		sup_exemption_type_stage_id,
		exemption_type_code,
		exemption_type_desc,
		modified_date,
		modified_user_id,
		extract_date,
		as_of_date,
		record_count,
		source_system_id
	FROM sup_exemption_type_stage
),
EXP_Default_Values AS (
	SELECT
	exemption_type_code,
	-- *INF*: IIF(ISNULL(exemption_type_code), 'N/A', exemption_type_code)
	IFF(exemption_type_code IS NULL, 'N/A', exemption_type_code) AS exemption_type_code_out,
	exemption_type_desc,
	-- *INF*: IIF(ISNULL(exemption_type_desc), 'N/A', exemption_type_desc)
	IFF(exemption_type_desc IS NULL, 'N/A', exemption_type_desc) AS exemption_type_desc_out
	FROM SQ_sup_exemption_type_stage
),
LKP_sup_workers_comp_exemption_type AS (
	SELECT
	sup_wc_exemption_type_id,
	wc_exemption_type_descript,
	wc_exemption_type_code
	FROM (
		SELECT sup_workers_comp_exemption_type.sup_wc_exemption_type_id as sup_wc_exemption_type_id, sup_workers_comp_exemption_type.wc_exemption_type_descript as wc_exemption_type_descript, 
		ltrim(rtrim(sup_workers_comp_exemption_type.wc_exemption_type_code)) as wc_exemption_type_code 
		FROM sup_workers_comp_exemption_type
		WHERE crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY wc_exemption_type_code ORDER BY sup_wc_exemption_type_id) = 1
),
EXP_detect_changes AS (
	SELECT
	LKP_sup_workers_comp_exemption_type.sup_wc_exemption_type_id AS lkp_sup_wc_exemption_type_id,
	LKP_sup_workers_comp_exemption_type.wc_exemption_type_descript AS lkp_wc_exemption_type_descript,
	EXP_Default_Values.exemption_type_code_out AS exemption_type_code,
	EXP_Default_Values.exemption_type_desc_out AS exemption_type_desc,
	-- *INF*: IIF(ISNULL(lkp_sup_wc_exemption_type_id), 'NEW', IIF(LTRIM(RTRIM(lkp_wc_exemption_type_descript)) != (LTRIM(RTRIM(exemption_type_desc))), 'UPDATE', 'NOCHANGE'))
	-- 
	IFF(lkp_sup_wc_exemption_type_id IS NULL, 'NEW', IFF(LTRIM(RTRIM(lkp_wc_exemption_type_descript)) != ( LTRIM(RTRIM(exemption_type_desc)) ), 'UPDATE', 'NOCHANGE')) AS v_CHANGED_FLAG,
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
	LEFT JOIN LKP_sup_workers_comp_exemption_type
	ON LKP_sup_workers_comp_exemption_type.wc_exemption_type_code = EXP_Default_Values.exemption_type_code_out
),
FIL_sup_workers_comp_exemption_type AS (
	SELECT
	exemption_type_code, 
	exemption_type_desc, 
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
TGT_sup_workers_comp_exemption_type_INSERT AS (
	INSERT INTO sup_workers_comp_exemption_type
	(wc_exemption_type_code, wc_exemption_type_descript, crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date)
	SELECT 
	exemption_type_code AS WC_EXEMPTION_TYPE_CODE, 
	exemption_type_desc AS WC_EXEMPTION_TYPE_DESCRIPT, 
	CRRNT_SNPSHT_FLAG, 
	AUDIT_ID, 
	EFF_FROM_DATE, 
	EFF_TO_DATE, 
	SOURCE_SYS_ID, 
	CREATED_DATE, 
	MODIFIED_DATE
	FROM FIL_sup_workers_comp_exemption_type
),
SQ_sup_workers_comp_exemption_type AS (
	SELECT a.sup_wc_exemption_type_id,
	                   a.wc_exemption_type_code, 
	                   a.eff_from_date, 
	                   a.eff_to_date 
	FROM
	  @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_workers_comp_exemption_type a
	
	WHERE EXISTS(SELECT 1			
			FROM  @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_workers_comp_exemption_type b
			WHERE source_sys_id= '@{pipeline().parameters.SOURCE_SYSTEM_ID}'  AND crrnt_snpsht_flag = 1
			AND a.wc_exemption_type_code = b.wc_exemption_type_code
			GROUP BY wc_exemption_type_code
			HAVING COUNT(*) > 1)
	ORDER BY wc_exemption_type_code, eff_from_date  DESC
),
EXP_lag_eff_from_date AS (
	SELECT
	sup_wc_exemption_type_id,
	wc_exemption_type_code,
	eff_from_date,
	eff_to_date AS orig_eff_to_date,
	-- *INF*: DECODE(TRUE,
	-- 	wc_exemption_type_code= v_Prev_row_wc_exemption_type_code, ADD_TO_DATE(v_prev_eff_from_date,'SS',-1),
	-- 	orig_eff_to_date)
	-- 	
	DECODE(TRUE,
		wc_exemption_type_code = v_Prev_row_wc_exemption_type_code, ADD_TO_DATE(v_prev_eff_from_date, 'SS', - 1),
		orig_eff_to_date) AS v_eff_to_date,
	v_eff_to_date AS eff_to_date,
	wc_exemption_type_code AS v_Prev_row_wc_exemption_type_code,
	eff_from_date AS v_prev_eff_from_date,
	0 AS crrnt_snpsht_flag,
	SYSDATE AS modified_date
	FROM SQ_sup_workers_comp_exemption_type
),
FIL_First_rown_inAKGroup AS (
	SELECT
	sup_wc_exemption_type_id, 
	orig_eff_to_date, 
	eff_to_date, 
	crrnt_snpsht_flag, 
	modified_date
	FROM EXP_lag_eff_from_date
	WHERE orig_eff_to_date != eff_to_date
),
UPD_sup_workers_comp_exemption_type AS (
	SELECT
	sup_wc_exemption_type_id, 
	eff_to_date, 
	crrnt_snpsht_flag, 
	modified_date
	FROM FIL_First_rown_inAKGroup
),
TGT_sup_workers_comp_exemption_type_UPDATE AS (
	MERGE INTO sup_workers_comp_exemption_type AS T
	USING UPD_sup_workers_comp_exemption_type AS S
	ON T.sup_wc_exemption_type_id = S.sup_wc_exemption_type_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snpsht_flag, T.eff_to_date = S.eff_to_date, T.modified_date = S.modified_date
),