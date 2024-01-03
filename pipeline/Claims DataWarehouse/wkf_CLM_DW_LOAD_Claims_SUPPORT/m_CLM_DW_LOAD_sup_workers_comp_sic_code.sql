WITH
SQ_gtam_wbsiccod_stage AS (
	SELECT
		gtam_wbsiccod_stage_id,
		sic_code_number,
		sic_code_description,
		extract_date,
		as_of_date,
		record_count,
		source_system_id
	FROM gtam_wbsiccod_stage
),
EXP_Default_Values AS (
	SELECT
	sic_code_number,
	sic_code_description,
	-- *INF*: IIF(ISNULL(sic_code_number), 'N/A', LTRIM(RTRIM(sic_code_number)))
	IFF(sic_code_number IS NULL, 'N/A', LTRIM(RTRIM(sic_code_number))) AS sic_code_number_OUT,
	-- *INF*: IIF(ISNULL(sic_code_description), 'N/A',  LTRIM(RTRIM(sic_code_description)))
	IFF(sic_code_description IS NULL, 'N/A', LTRIM(RTRIM(sic_code_description))) AS sic_code_description_OUT
	FROM SQ_gtam_wbsiccod_stage
),
LKP_WC_SUP_SIC_CODE AS (
	SELECT
	sup_wc_sic_code_id,
	sic_code,
	sic_code_descript,
	IN_sic_code_number
	FROM (
		SELECT sup_workers_comp_sic_code.sup_wc_sic_code_id as sup_wc_sic_code_id,  LTRIM(RTRIM(sup_workers_comp_sic_code.sic_code_descript)) as sic_code_descript,  LTRIM(RTRIM(sup_workers_comp_sic_code.sic_code)) as sic_code FROM sup_workers_comp_sic_code
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY sic_code ORDER BY sup_wc_sic_code_id) = 1
),
EXP_detect_changes AS (
	SELECT
	LKP_WC_SUP_SIC_CODE.sic_code AS old_sic_code_number,
	LKP_WC_SUP_SIC_CODE.sic_code_descript AS old_sic_code_description,
	EXP_Default_Values.sic_code_description_OUT,
	EXP_Default_Values.sic_code_number_OUT,
	-- *INF*: IIF(ISNULL(old_sic_code_number), 'NEW', IIF(old_sic_code_description != sic_code_description_OUT, 'UPDATE', 'NOCHANGE'))
	-- 
	IFF(old_sic_code_number IS NULL, 'NEW', IFF(old_sic_code_description != sic_code_description_OUT, 'UPDATE', 'NOCHANGE')) AS v_CHANGED_FLAG,
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
	LEFT JOIN LKP_WC_SUP_SIC_CODE
	ON LKP_WC_SUP_SIC_CODE.sic_code = EXP_Default_Values.sic_code_number_OUT
),
FIL_sup_wc_sic_code AS (
	SELECT
	sic_code_description_OUT, 
	sic_code_number_OUT, 
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
sup_workers_comp_sic_code_insert AS (
	INSERT INTO sup_workers_comp_sic_code
	(sic_code, sic_code_descript, crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date)
	SELECT 
	sic_code_number_OUT AS SIC_CODE, 
	sic_code_description_OUT AS SIC_CODE_DESCRIPT, 
	CRRNT_SNPSHT_FLAG, 
	AUDIT_ID, 
	EFF_FROM_DATE, 
	EFF_TO_DATE, 
	SOURCE_SYS_ID, 
	CREATED_DATE, 
	MODIFIED_DATE
	FROM FIL_sup_wc_sic_code
),
SQ_sup_workers_comp_sic_code AS (
	SELECT a.sup_wc_sic_code_id, a.sic_code, a.eff_from_date, a.eff_to_date 
	FROM
	 @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_workers_comp_sic_code a
	WHERE EXISTS(SELECT 1			
			FROM  @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_workers_comp_sic_code b
			WHERE source_sys_id= '@{pipeline().parameters.SOURCE_SYSTEM_ID}'  AND crrnt_snpsht_flag = 1
			AND a.sic_code = b.sic_code
			GROUP BY sic_code
			HAVING COUNT(*) > 1)
	ORDER BY sic_code, eff_from_date  DESC
),
EXP_lag_eff_from_date AS (
	SELECT
	sup_wc_sic_code_id,
	sic_code,
	eff_from_date,
	eff_to_date AS orig_eff_to_date,
	-- *INF*: DECODE(TRUE,
	-- 	sic_code = v_Prev_row_ins_line_code, ADD_TO_DATE(v_prev_eff_from_date,'SS',-1),
	-- 	orig_eff_to_date)
	-- 	
	DECODE(TRUE,
	sic_code = v_Prev_row_ins_line_code, ADD_TO_DATE(v_prev_eff_from_date, 'SS', - 1),
	orig_eff_to_date) AS v_eff_to_date,
	v_eff_to_date AS eff_to_date,
	sic_code AS v_Prev_row_ins_line_code,
	eff_from_date AS v_prev_eff_from_date,
	0 AS crrnt_snpsht_flag,
	SYSDATE AS modified_date
	FROM SQ_sup_workers_comp_sic_code
),
FIL_First_rown_inAKGroup AS (
	SELECT
	sup_wc_sic_code_id, 
	orig_eff_to_date, 
	eff_to_date, 
	crrnt_snpsht_flag, 
	modified_date
	FROM EXP_lag_eff_from_date
	WHERE orig_eff_to_date != eff_to_date
),
UPD_sup_insurance_line AS (
	SELECT
	sup_wc_sic_code_id, 
	eff_to_date, 
	crrnt_snpsht_flag, 
	modified_date
	FROM FIL_First_rown_inAKGroup
),
sup_workers_comp_sic_code1 AS (
	MERGE INTO sup_workers_comp_sic_code AS T
	USING UPD_sup_insurance_line AS S
	ON T.sup_wc_sic_code_id = S.sup_wc_sic_code_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snpsht_flag, T.eff_to_date = S.eff_to_date, T.modified_date = S.modified_date
),