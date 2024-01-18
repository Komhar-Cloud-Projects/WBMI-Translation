WITH
SQ_aia12_desc_stage AS (
	SELECT 
	RTRIM(aia12_desc_stage.rec_code), 
	RTRIM(aia12_desc_stage.description) 
	FROM
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.aia12_desc_stage aia12_desc_stage
),
LKP_WC_BODY_PART AS (
	SELECT
	NewLookupRow,
	sup_wc_body_part_id,
	rec_code,
	body_part_code,
	description,
	body_part_descript
	FROM (
		SELECT 
		sup_workers_comp_body_part.sup_wc_body_part_id as sup_wc_body_part_id, RTRIM(sup_workers_comp_body_part.body_part_descript) as body_part_descript, RTRIM(sup_workers_comp_body_part.body_part_code) as body_part_code  
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_workers_comp_body_part AS sup_workers_comp_body_part
		WHERE crrnt_snpsht_flag = 1 AND
		SOURCE_SYS_ID = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY body_part_code ORDER BY NewLookupRow) = 1
),
FIL_NEW_UPDATED_ROWS AS (
	SELECT
	NewLookupRow, 
	sup_wc_body_part_id, 
	body_part_code, 
	body_part_descript
	FROM LKP_WC_BODY_PART
	WHERE NewLookupRow = 1 OR
NewLookupRow = 2
),
EXP_AUDIT_FIELDS AS (
	SELECT
	NewLookupRow,
	1 AS crrnt_snpsht_flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	-- *INF*: IIF(NewLookupRow=1,
	-- 	TO_DATE('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'),SYSDATE)
	IFF(
	    NewLookupRow = 1, TO_TIMESTAMP('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'),
	    CURRENT_TIMESTAMP
	) AS eff_from_date,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_TIMESTAMP('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS eff_to_date,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS source_sys_id,
	sysdate AS created_date,
	sup_wc_body_part_id,
	body_part_code,
	body_part_descript
	FROM FIL_NEW_UPDATED_ROWS
),
sup_workers_comp_body_part_insert AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_workers_comp_body_part
	(body_part_code, body_part_descript, crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date)
	SELECT 
	BODY_PART_CODE, 
	BODY_PART_DESCRIPT, 
	CRRNT_SNPSHT_FLAG, 
	AUDIT_ID, 
	EFF_FROM_DATE, 
	EFF_TO_DATE, 
	SOURCE_SYS_ID, 
	CREATED_DATE, 
	created_date AS MODIFIED_DATE
	FROM EXP_AUDIT_FIELDS
),
SQ_sup_workers_comp_body_part AS (
	SELECT 
	sup_workers_comp_body_part.sup_wc_body_part_id, sup_workers_comp_body_part.body_part_code, sup_workers_comp_body_part.eff_from_date, sup_workers_comp_body_part.eff_to_date 
	FROM
	@{pipeline().parameters.TARGET_TABLE_OWNER}.sup_workers_comp_body_part 
	WHERE EXISTS
	(
	SELECT 1 FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_workers_comp_body_part sup_workers_comp_body_part2 
	WHERE
	crrnt_snpsht_flag = 1 AND sup_workers_comp_body_part2.body_part_code = sup_workers_comp_body_part.body_part_code
	GROUP BY sup_workers_comp_body_part2.body_part_code HAVING COUNT(*) > 1
	)
	order by 
	sup_workers_comp_body_part.body_part_code,
	sup_workers_comp_body_part.eff_from_date  desc
),
EXP_Lag_eff_from_date111 AS (
	SELECT
	sup_wc_body_part_id,
	body_part_code,
	eff_from_date,
	eff_to_date AS orig_eff_to_date,
	-- *INF*: DECODE(TRUE,
	-- 	body_part_code = v_PREV_ROW_occurrence_key, ADD_TO_DATE(v_PREV_ROW_eff_from_date,'SS',-1),
	-- 	orig_eff_to_date)
	DECODE(
	    TRUE,
	    body_part_code = v_PREV_ROW_occurrence_key, DATEADD(SECOND,- 1,v_PREV_ROW_eff_from_date),
	    orig_eff_to_date
	) AS v_eff_to_date,
	v_eff_to_date AS eff_to_date,
	eff_from_date AS v_PREV_ROW_eff_from_date,
	body_part_code AS v_PREV_ROW_occurrence_key,
	SYSDATE AS modified_date,
	0 AS crrnt_snpsht_flag
	FROM SQ_sup_workers_comp_body_part
),
FIL_First_Row_in_AK_Group1 AS (
	SELECT
	sup_wc_body_part_id, 
	orig_eff_to_date, 
	eff_to_date, 
	modified_date, 
	crrnt_snpsht_flag
	FROM EXP_Lag_eff_from_date111
	WHERE orig_eff_to_date <> eff_to_date
),
UPD_CRRNT_SNPSHT_FLG AS (
	SELECT
	sup_wc_body_part_id, 
	orig_eff_to_date, 
	eff_to_date, 
	modified_date, 
	crrnt_snpsht_flag
	FROM FIL_First_Row_in_AK_Group1
),
sup_workers_comp_body_part_update AS (
	MERGE INTO sup_workers_comp_body_part AS T
	USING UPD_CRRNT_SNPSHT_FLG AS S
	ON T.sup_wc_body_part_id = S.sup_wc_body_part_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snpsht_flag, T.eff_to_date = S.eff_to_date, T.modified_date = S.modified_date
),