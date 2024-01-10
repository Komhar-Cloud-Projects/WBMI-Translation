WITH
SQ_gtam_tm523c_stage AS (
	SELECT
		tm523c_stage_id,
		table_fld,
		key_len,
		line_of_business,
		insurance_line,
		risk_unit,
		language_indicator,
		data_len,
		risk_unit_literal,
		extract_date,
		as_of_date,
		record_count,
		source_sytem_id
	FROM gtam_tm523c_stage
),
EXP_Default_Values AS (
	SELECT
	risk_unit AS IN_risk_unit,
	risk_unit_literal AS IN_risk_unit_literal,
	insurance_line AS IN_insurance_line,
	-- *INF*: iif(isnull(IN_insurance_line),'N/A',ltrim(rtrim(IN_insurance_line)))
	IFF(IN_insurance_line IS NULL, 'N/A', ltrim(rtrim(IN_insurance_line))) AS INSURANCE_LINE_OUT,
	-- *INF*: IIF(ISNULL(IN_risk_unit), 'N/A', ltrim(rtrim(IN_risk_unit)))
	IFF(IN_risk_unit IS NULL, 'N/A', ltrim(rtrim(IN_risk_unit))) AS RISK_UNIT_CODE_OUT,
	-- *INF*: IIF(ISNULL(IN_risk_unit_literal), 'N/A', IN_risk_unit_literal)
	IFF(IN_risk_unit_literal IS NULL, 'N/A', IN_risk_unit_literal) AS RISK_UNIT_LITERAL_OUT
	FROM SQ_gtam_tm523c_stage
),
LKP_sup_risk_unit AS (
	SELECT
	sup_risk_unit_id,
	risk_unit_descript,
	risk_unit_code,
	ins_line
	FROM (
		SELECT 
		a.sup_risk_unit_id as sup_risk_unit_id, 
		ltrim(rtrim(a.risk_unit_descript)) as risk_unit_descript, 
		ltrim(rtrim(a.risk_unit_code)) as risk_unit_code, 
		ltrim(rtrim(a.ins_line)) as ins_line 
		FROM sup_risk_unit a
		where a.crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY risk_unit_code,ins_line ORDER BY sup_risk_unit_id) = 1
),
EXP_detect_changes AS (
	SELECT
	LKP_sup_risk_unit.sup_risk_unit_id AS old_sup_risk_unit_id,
	LKP_sup_risk_unit.risk_unit_descript AS old_risk_unit_descript,
	EXP_Default_Values.INSURANCE_LINE_OUT,
	EXP_Default_Values.RISK_UNIT_CODE_OUT,
	EXP_Default_Values.RISK_UNIT_LITERAL_OUT,
	-- *INF*: IIF(ISNULL(old_sup_risk_unit_id), 'NEW', 
	-- IIF(ltrim(rtrim(old_risk_unit_descript)) != ltrim(rtrim(RISK_UNIT_LITERAL_OUT)), 
	-- 'UPDATE', 'NOCHANGE'))
	-- 
	IFF(old_sup_risk_unit_id IS NULL, 'NEW', IFF(ltrim(rtrim(old_risk_unit_descript)) != ltrim(rtrim(RISK_UNIT_LITERAL_OUT)), 'UPDATE', 'NOCHANGE')) AS v_CHANGED_FLAG,
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
	LEFT JOIN LKP_sup_risk_unit
	ON LKP_sup_risk_unit.risk_unit_code = EXP_Default_Values.RISK_UNIT_CODE_OUT AND LKP_sup_risk_unit.ins_line = EXP_Default_Values.INSURANCE_LINE_OUT
),
FIL_sup_insurance_line_insert AS (
	SELECT
	INSURANCE_LINE_OUT, 
	RISK_UNIT_CODE_OUT, 
	RISK_UNIT_LITERAL_OUT, 
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
sup_risk_unit_Insert AS (
	INSERT INTO sup_risk_unit
	(risk_unit_code, risk_unit_descript, ins_line, crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date)
	SELECT 
	RISK_UNIT_CODE_OUT AS RISK_UNIT_CODE, 
	RISK_UNIT_LITERAL_OUT AS RISK_UNIT_DESCRIPT, 
	INSURANCE_LINE_OUT AS INS_LINE, 
	CRRNT_SNPSHT_FLAG, 
	AUDIT_ID, 
	EFF_FROM_DATE, 
	EFF_TO_DATE, 
	SOURCE_SYS_ID, 
	CREATED_DATE, 
	MODIFIED_DATE
	FROM FIL_sup_insurance_line_insert
),
SQ_sup_risk_unit AS (
	SELECT a.sup_risk_unit_id, 
	a.risk_unit_code, 
	a.ins_line,
	a.eff_from_date, 
	a.eff_to_date 
	FROM
	 @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_risk_unit a
	WHERE EXISTS(SELECT 1			
			FROM  @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_risk_unit b
			WHERE source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'  AND crrnt_snpsht_flag = 1
			AND a.risk_unit_code = b.risk_unit_code AND
	                        a.ins_line = b.ins_line
			GROUP BY risk_unit_code,ins_line
			HAVING COUNT(*) > 1)
	ORDER BY risk_unit_code, ins_line, eff_from_date  DESC
),
EXP_lag_eff_from_date AS (
	SELECT
	sup_risk_unit_id,
	risk_unit_code,
	ins_line,
	eff_from_date,
	eff_to_date AS orig_eff_to_date,
	-- *INF*: DECODE(TRUE,
	-- 	risk_unit_code = v_Prev_row_risk_unit_code AND ins_line = v_Prev_row_ins_line,
	-- 	 ADD_TO_DATE(v_prev_eff_from_date,'SS',-1),
	-- 	orig_eff_to_date)
	-- 	
	DECODE(TRUE,
		risk_unit_code = v_Prev_row_risk_unit_code AND ins_line = v_Prev_row_ins_line, ADD_TO_DATE(v_prev_eff_from_date, 'SS', - 1),
		orig_eff_to_date) AS v_eff_to_date,
	v_eff_to_date AS eff_to_date,
	risk_unit_code AS v_Prev_row_risk_unit_code,
	ins_line AS v_Prev_row_ins_line,
	eff_from_date AS v_prev_eff_from_date,
	0 AS crrnt_snpsht_flag,
	SYSDATE AS modified_date
	FROM SQ_sup_risk_unit
),
FIL_First_rown_inAKGroup AS (
	SELECT
	sup_risk_unit_id, 
	orig_eff_to_date, 
	eff_to_date, 
	crrnt_snpsht_flag, 
	modified_date
	FROM EXP_lag_eff_from_date
	WHERE orig_eff_to_date != eff_to_date
),
UPD_sup_insurance_line AS (
	SELECT
	sup_risk_unit_id, 
	eff_to_date, 
	crrnt_snpsht_flag, 
	modified_date
	FROM FIL_First_rown_inAKGroup
),
sup_risk_unit_update AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_risk_unit AS T
	USING UPD_sup_insurance_line AS S
	ON T.sup_risk_unit_id = S.sup_risk_unit_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snpsht_flag, T.eff_to_date = S.eff_to_date, T.modified_date = S.modified_date
),