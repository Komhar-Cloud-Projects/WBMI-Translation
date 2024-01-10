WITH
SQ_aia56_desc_stage AS (
	SELECT
		aia56_desc_stage_id,
		rec_code,
		description,
		extract_date,
		as_of_date,
		record_count,
		source_system_id
	FROM aia56_desc_stage
),
EXP_Default_Values AS (
	SELECT
	rec_code,
	-- *INF*: iif(isnull(rec_code),'N/A',rec_code)
	IFF(rec_code IS NULL, 'N/A', rec_code) AS rec_code_OUT,
	description,
	-- *INF*: iif(isnull(description),'N/A',description)
	IFF(description IS NULL, 'N/A', description) AS descript_OUT
	FROM SQ_aia56_desc_stage
),
LKP_sup_workers_comp_cause_of_injury AS (
	SELECT
	IN_rec_code,
	sup_wc_cause_of_inj_id,
	cause_of_inj_code,
	cause_of_inj_descript
	FROM (
		SELECT sup_workers_comp_cause_of_injury.sup_wc_cause_of_inj_id as sup_wc_cause_of_inj_id, 
		LTRIM(RTRIM(sup_workers_comp_cause_of_injury.cause_of_inj_descript)) as cause_of_inj_descript,
		 LTRIM(RTRIM(sup_workers_comp_cause_of_injury.cause_of_inj_code)) as cause_of_inj_code 
		FROM sup_workers_comp_cause_of_injury WHERE CRRNT_SNPSHT_FLAG = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY cause_of_inj_code ORDER BY IN_rec_code) = 1
),
EXP_Detect_Changes AS (
	SELECT
	LKP_sup_workers_comp_cause_of_injury.sup_wc_cause_of_inj_id AS OLD_sup_wc_cause_of_inj_id,
	LKP_sup_workers_comp_cause_of_injury.cause_of_inj_descript AS OLD_cause_of_inj_descript,
	EXP_Default_Values.rec_code_OUT,
	EXP_Default_Values.descript_OUT,
	-- *INF*: IIF(ISNULL(OLD_sup_wc_cause_of_inj_id), 'NEW', IIF(LTRIM(RTRIM(OLD_cause_of_inj_descript)) != (LTRIM(RTRIM(descript_OUT))), 'UPDATE', 'NOCHANGE'))
	IFF(OLD_sup_wc_cause_of_inj_id IS NULL, 'NEW', IFF(LTRIM(RTRIM(OLD_cause_of_inj_descript)) != ( LTRIM(RTRIM(descript_OUT)) ), 'UPDATE', 'NOCHANGE')) AS V_changed_flag,
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
	LEFT JOIN LKP_sup_workers_comp_cause_of_injury
	ON LKP_sup_workers_comp_cause_of_injury.cause_of_inj_code = EXP_Default_Values.rec_code_OUT
),
FIL_sup_workers_comp_nature_of_injury AS (
	SELECT
	rec_code_OUT, 
	descript_OUT, 
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
sup_workers_comp_cause_of_injury_INSERT AS (
	INSERT INTO sup_workers_comp_cause_of_injury
	(cause_of_inj_code, cause_of_inj_descript, crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date)
	SELECT 
	rec_code_OUT AS CAUSE_OF_INJ_CODE, 
	descript_OUT AS CAUSE_OF_INJ_DESCRIPT, 
	CRRNT_SNPSHT_FLAG, 
	AUDIT_ID, 
	EFF_FROM_DATE, 
	EFF_TO_DATE, 
	SOURCE_SYS_ID, 
	CREATED_DATE, 
	MODIFIED_DATE
	FROM FIL_sup_workers_comp_nature_of_injury
),
SQ_sup_workers_comp_cause_of_injury AS (
	SELECT a.sup_wc_cause_of_inj_id, 
	a.cause_of_inj_code,
	a.eff_from_date,
	a.eff_to_date 
	FROM
	  @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_workers_comp_cause_of_injury a
	
	WHERE EXISTS(SELECT 1			
			FROM  @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_workers_comp_cause_of_injury   b
			WHERE source_sys_id= '@{pipeline().parameters.SOURCE_SYSTEM_ID}'  AND crrnt_snpsht_flag = 1
			AND a.cause_of_inj_code = b.cause_of_inj_code
	             GROUP BY cause_of_inj_code		
	             HAVING COUNT(*) > 1)
	ORDER BY cause_of_inj_code, eff_from_date  DESC
),
EXP_Lag_Eff_From_Date AS (
	SELECT
	sup_wc_cause_of_inj_id,
	cause_of_inj_code,
	eff_from_date,
	eff_to_date AS orig_eff_to_date,
	-- *INF*: DECODE(TRUE,
	-- 	cause_of_inj_code= v_prev_row_cause_of_inj_code, ADD_TO_DATE(v_prev_eff_from_date,'SS',-1),
	-- 	orig_eff_to_date)
	-- 	
	DECODE(TRUE,
		cause_of_inj_code = v_prev_row_cause_of_inj_code, ADD_TO_DATE(v_prev_eff_from_date, 'SS', - 1),
		orig_eff_to_date) AS v_eff_to_date,
	v_eff_to_date AS eff_to_date,
	cause_of_inj_code AS v_prev_row_cause_of_inj_code,
	eff_from_date AS v_prev_eff_from_date,
	0 AS crrnt_snpsht_flag,
	SYSDATE AS modified_date
	FROM SQ_sup_workers_comp_cause_of_injury
),
FIL_First_Row_In_AK_Group AS (
	SELECT
	sup_wc_cause_of_inj_id, 
	orig_eff_to_date, 
	eff_to_date, 
	crrnt_snpsht_flag, 
	modified_date
	FROM EXP_Lag_Eff_From_Date
	WHERE orig_eff_to_date !=eff_to_date
),
UPD_sup_workers_comp_cause_of_injury AS (
	SELECT
	sup_wc_cause_of_inj_id, 
	eff_to_date, 
	crrnt_snpsht_flag, 
	modified_date
	FROM FIL_First_Row_In_AK_Group
),
sup_workers_comp_cause_of_UPDATE AS (
	MERGE INTO sup_workers_comp_cause_of_injury AS T
	USING UPD_sup_workers_comp_cause_of_injury AS S
	ON T.sup_wc_cause_of_inj_id = S.sup_wc_cause_of_inj_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snpsht_flag, T.eff_to_date = S.eff_to_date, T.modified_date = S.modified_date
),