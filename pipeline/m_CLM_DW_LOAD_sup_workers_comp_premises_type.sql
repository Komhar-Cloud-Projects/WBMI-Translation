WITH
SQ_sup_premises_type_stage AS (
	SELECT
		sup_premises_type_stage_id,
		premises_code,
		premises_desc,
		modified_date,
		modified_user_id,
		extract_date,
		as_of_date,
		record_count,
		source_system_id
	FROM sup_premises_type_stage
),
EXP_Default_Values AS (
	SELECT
	premises_code,
	-- *INF*: iif(isnull(premises_code),'N/A',premises_code)
	IFF(premises_code IS NULL, 'N/A', premises_code) AS premises_code_OUT,
	premises_desc AS premises_descript,
	-- *INF*: iif(isnull(premises_descript),'N/A',premises_descript)
	IFF(premises_descript IS NULL, 'N/A', premises_descript) AS premises_descript_OUT
	FROM SQ_sup_premises_type_stage
),
LKP_sup_workers_comp_premises_type AS (
	SELECT
	sup_wc_premises_type_id,
	premises_code,
	premises_descript,
	IN_premises_code
	FROM (
		SELECT sup_workers_comp_premises_type.sup_wc_premises_type_id as sup_wc_premises_type_id, 
		LTRIM(RTRIM(sup_workers_comp_premises_type.premises_descript)) as premises_descript, 
		LTRIM(RTRIM(sup_workers_comp_premises_type.premises_code)) as premises_code
		 FROM sup_workers_comp_premises_type
		WHERE  crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY premises_code ORDER BY sup_wc_premises_type_id) = 1
),
EXP_Detect_Changes AS (
	SELECT
	LKP_sup_workers_comp_premises_type.sup_wc_premises_type_id AS OLD_sup_wc_premises_type_id,
	LKP_sup_workers_comp_premises_type.premises_descript AS OLD_premises_descript,
	EXP_Default_Values.premises_code_OUT,
	EXP_Default_Values.premises_descript_OUT,
	-- *INF*: IIF(ISNULL(OLD_sup_wc_premises_type_id), 'NEW', IIF(LTRIM(RTRIM(OLD_premises_descript)) != (LTRIM(RTRIM(premises_descript_OUT))), 'UPDATE', 'NOCHANGE'))
	IFF(
	    OLD_sup_wc_premises_type_id IS NULL, 'NEW',
	    IFF(
	        LTRIM(RTRIM(OLD_premises_descript)) != (LTRIM(RTRIM(premises_descript_OUT))),
	        'UPDATE',
	        'NOCHANGE'
	    )
	) AS V_changed_flag,
	V_changed_flag AS CHANGED_FLAG,
	1 AS crrnt_snpsht_flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	-- *INF*: iif(V_changed_flag='NEW',
	-- 	to_date('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'),sysdate)
	IFF(
	    V_changed_flag = 'NEW', TO_TIMESTAMP('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'),
	    CURRENT_TIMESTAMP
	) AS eff_from_date,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_TIMESTAMP('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS eff_to_date,
	SYSDATE AS created_date,
	SYSDATE AS modified_date,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS source_sys_id
	FROM EXP_Default_Values
	LEFT JOIN LKP_sup_workers_comp_premises_type
	ON LKP_sup_workers_comp_premises_type.premises_code = EXP_Default_Values.premises_code_OUT
),
FIL_sup_workers_comp_premises_type AS (
	SELECT
	premises_code_OUT, 
	premises_descript_OUT, 
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
sup_workers_comp_premises_type_INSERT AS (
	INSERT INTO sup_workers_comp_premises_type
	(premises_code, premises_descript, crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date)
	SELECT 
	premises_code_OUT AS PREMISES_CODE, 
	premises_descript_OUT AS PREMISES_DESCRIPT, 
	CRRNT_SNPSHT_FLAG, 
	AUDIT_ID, 
	EFF_FROM_DATE, 
	EFF_TO_DATE, 
	SOURCE_SYS_ID, 
	CREATED_DATE, 
	MODIFIED_DATE
	FROM FIL_sup_workers_comp_premises_type
),
SQ_sup_workers_comp_premises_type AS (
	SELECT a.sup_wc_premises_type_id,
	a.premises_code, 
	a.eff_from_date,
	a.eff_to_date 
	FROM
	  @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_workers_comp_premises_type a
	
	WHERE EXISTS(SELECT 1			
			FROM  @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_workers_comp_premises_type b
			WHERE source_sys_id= '@{pipeline().parameters.SOURCE_SYSTEM_ID}'  AND crrnt_snpsht_flag = 1
			AND a.premises_code = b.premises_code
			GROUP BY premises_code
			HAVING COUNT(*) > 1)
	ORDER BY premises_code, eff_from_date  DESC
),
EXP_Lag_Eff_From_Date AS (
	SELECT
	sup_wc_premises_type_id,
	premises_code,
	eff_from_date,
	eff_to_date AS orig_eff_to_date,
	-- *INF*: DECODE(TRUE,
	-- 	premises_code= v_prev_row_premises_code, ADD_TO_DATE(v_prev_eff_from_date,'SS',-1),
	-- 	orig_eff_to_date)
	-- 	
	DECODE(
	    TRUE,
	    premises_code = v_prev_row_premises_code, DATEADD(SECOND,- 1,v_prev_eff_from_date),
	    orig_eff_to_date
	) AS v_eff_to_date,
	v_eff_to_date AS eff_to_date,
	premises_code AS v_prev_row_premises_code,
	eff_from_date AS v_prev_eff_from_date,
	0 AS crrnt_snpsht_flag,
	SYSDATE AS modified_date
	FROM SQ_sup_workers_comp_premises_type
),
FIL_First_Row_In_AK_Group AS (
	SELECT
	sup_wc_premises_type_id, 
	orig_eff_to_date, 
	eff_to_date, 
	crrnt_snpsht_flag, 
	modified_date
	FROM EXP_Lag_Eff_From_Date
	WHERE orig_eff_to_date !=eff_to_date
),
UPD_sup_workers_comp_premises_type AS (
	SELECT
	sup_wc_premises_type_id, 
	eff_to_date, 
	crrnt_snpsht_flag, 
	modified_date
	FROM FIL_First_Row_In_AK_Group
),
sup_workers_comp_premises_UPDATE AS (
	MERGE INTO sup_workers_comp_premises_type AS T
	USING UPD_sup_workers_comp_premises_type AS S
	ON T.sup_wc_premises_type_id = S.sup_wc_premises_type_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snpsht_flag, T.eff_to_date = S.eff_to_date, T.modified_date = S.modified_date
),