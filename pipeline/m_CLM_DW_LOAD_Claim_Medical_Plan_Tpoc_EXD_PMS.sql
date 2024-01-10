WITH
SQ_clm_med_plan_type_stage AS (
	SELECT
		clm_med_plan_type_stage_id,
		cms_doc_cntl_num,
		injured_party_id,
		wbm_plan_ins_type,
		state_venue,
		med_oblig_to_clmt,
		orm_terminate_dt,
		no_fault_ins_limit,
		exhaust_limit_dt,
		tpoc_date1,
		tpoc_amount1,
		tpoc_fund_dlay_dt1,
		tpoc_date2,
		tpoc_amount2,
		tpoc_fund_dlay_dt2,
		tpoc_date3,
		tpoc_amount3,
		tpoc_fund_dlay_dt3,
		tpoc_date4,
		tpoc_amount4,
		tpoc_fund_dlay_dt4,
		tpoc_date5,
		tpoc_amount5,
		tpoc_fund_dlay_dt5,
		extract_date,
		as_of_date,
		record_count,
		source_system_id,
		plan_type_deleted
	FROM clm_med_plan_type_stage
),
LKP_claim_med_plan_ak_id AS (
	SELECT
	claim_med_plan_ak_id,
	injured_party_id,
	wbmi_plan_ins_type
	FROM (
		SELECT 
		CMP.claim_med_plan_ak_id as claim_med_plan_ak_id, 
		CM.injured_party_id as injured_party_id, 
		CMP.wbmi_plan_ins_type as wbmi_plan_ins_type 
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_medical_plan CMP, @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_medical CM
		WHERE 
		CM.claim_med_ak_id  = CMP.claim_med_ak_id
		AND CM.crrnt_snpsht_flag = 1
		AND CMP.crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY injured_party_id,wbmi_plan_ins_type ORDER BY claim_med_plan_ak_id) = 1
),
EXP_Values AS (
	SELECT
	LKP_claim_med_plan_ak_id.claim_med_plan_ak_id AS in_claim_med_plan_ak_id,
	SQ_clm_med_plan_type_stage.tpoc_date1,
	SQ_clm_med_plan_type_stage.tpoc_amount1,
	SQ_clm_med_plan_type_stage.tpoc_fund_dlay_dt1,
	1 AS code1,
	SQ_clm_med_plan_type_stage.tpoc_date2,
	SQ_clm_med_plan_type_stage.tpoc_amount2,
	SQ_clm_med_plan_type_stage.tpoc_fund_dlay_dt2,
	2 AS code2,
	SQ_clm_med_plan_type_stage.tpoc_date3,
	SQ_clm_med_plan_type_stage.tpoc_amount3,
	SQ_clm_med_plan_type_stage.tpoc_fund_dlay_dt3,
	3 AS code3,
	SQ_clm_med_plan_type_stage.tpoc_date4,
	SQ_clm_med_plan_type_stage.tpoc_amount4,
	SQ_clm_med_plan_type_stage.tpoc_fund_dlay_dt4,
	4 AS code4,
	SQ_clm_med_plan_type_stage.tpoc_date5,
	SQ_clm_med_plan_type_stage.tpoc_amount5,
	SQ_clm_med_plan_type_stage.tpoc_fund_dlay_dt5,
	5 AS code5
	FROM SQ_clm_med_plan_type_stage
	LEFT JOIN LKP_claim_med_plan_ak_id
	ON LKP_claim_med_plan_ak_id.injured_party_id = SQ_clm_med_plan_type_stage.injured_party_id AND LKP_claim_med_plan_ak_id.wbmi_plan_ins_type = SQ_clm_med_plan_type_stage.wbm_plan_ins_type
),
UNI_Split_Record AS (
	SELECT in_claim_med_plan_ak_id AS claim_med_plan_ak_id, code1 AS tpoc_code, tpoc_date1 AS tpoc_date, tpoc_amount1 AS tpoc_amount, tpoc_fund_dlay_dt1 AS tpoc_delay_date
	FROM 
	UNION
	SELECT in_claim_med_plan_ak_id AS claim_med_plan_ak_id, code2 AS tpoc_code, tpoc_date2 AS tpoc_date, tpoc_amount2 AS tpoc_amount, tpoc_fund_dlay_dt2 AS tpoc_delay_date
	FROM 
	UNION
	SELECT in_claim_med_plan_ak_id AS claim_med_plan_ak_id, code3 AS tpoc_code, tpoc_date3 AS tpoc_date, tpoc_amount3 AS tpoc_amount, tpoc_fund_dlay_dt3 AS tpoc_delay_date
	FROM 
	UNION
	SELECT in_claim_med_plan_ak_id AS claim_med_plan_ak_id, code4 AS tpoc_code, tpoc_date4 AS tpoc_date, tpoc_amount4 AS tpoc_amount, tpoc_fund_dlay_dt4 AS tpoc_delay_date
	FROM 
	UNION
	SELECT in_claim_med_plan_ak_id AS claim_med_plan_ak_id, code5 AS tpoc_code, tpoc_date5 AS tpoc_date, tpoc_amount5 AS tpoc_amount, tpoc_fund_dlay_dt5 AS tpoc_delay_date
	FROM 
),
FIL_Valid_Codes AS (
	SELECT
	claim_med_plan_ak_id, 
	tpoc_code, 
	tpoc_date, 
	tpoc_amount, 
	tpoc_delay_date AS tpoc_fund_dlay_dt
	FROM UNI_Split_Record
	WHERE IIF(ISNULL(tpoc_date) AND ISNULL(tpoc_amount) AND ISNULL(tpoc_fund_dlay_dt),
FALSE,
TRUE)
),
EXP_Default_Values AS (
	SELECT
	claim_med_plan_ak_id,
	tpoc_code,
	tpoc_date,
	-- *INF*: IIF(ISNULL(tpoc_date), TO_DATE('1/1/1800','MM/DD/YYYY'), tpoc_date)
	IFF(tpoc_date IS NULL, TO_DATE('1/1/1800', 'MM/DD/YYYY'), tpoc_date) AS tpoc_date1,
	tpoc_amount,
	-- *INF*: IIF(ISNULL(tpoc_amount), 0.00, tpoc_amount)
	IFF(tpoc_amount IS NULL, 0.00, tpoc_amount) AS tpoc_amount1,
	tpoc_fund_dlay_dt,
	-- *INF*: IIF(ISNULL(tpoc_fund_dlay_dt), TO_DATE('1/1/1800','MM/DD/YYYY'), tpoc_fund_dlay_dt)
	IFF(tpoc_fund_dlay_dt IS NULL, TO_DATE('1/1/1800', 'MM/DD/YYYY'), tpoc_fund_dlay_dt) AS tpoc_fund_dlay_dt1
	FROM FIL_Valid_Codes
),
LKP_Target AS (
	SELECT
	claim_med_plan_tpoc_ak_id,
	tpoc_fund_delay_date,
	tpoc_date,
	tpoc_amt,
	claim_med_plan_ak_id,
	tpoc_code
	FROM (
		SELECT claim_medical_plan_tpoc.claim_med_plan_tpoc_ak_id as claim_med_plan_tpoc_ak_id, claim_medical_plan_tpoc.tpoc_fund_delay_date as tpoc_fund_delay_date, claim_medical_plan_tpoc.tpoc_date as tpoc_date, claim_medical_plan_tpoc.tpoc_amt as tpoc_amt, claim_medical_plan_tpoc.claim_med_plan_ak_id as claim_med_plan_ak_id, claim_medical_plan_tpoc.tpoc_code as tpoc_code 
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_medical_plan_tpoc
		WHERE crrnt_snpsht_flag =1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_med_plan_ak_id,tpoc_code ORDER BY claim_med_plan_tpoc_ak_id) = 1
),
EXP_Detect_Changes AS (
	SELECT
	LKP_Target.claim_med_plan_tpoc_ak_id AS lkp_claim_med_plan_tpoc_ak_id,
	LKP_Target.tpoc_fund_delay_date AS lkp_tpoc_fund_delay_date,
	LKP_Target.tpoc_date AS lkp_tpoc_date,
	LKP_Target.tpoc_amt AS lkp_tpoc_amt,
	EXP_Default_Values.claim_med_plan_ak_id,
	EXP_Default_Values.tpoc_code AS tpoc_code1,
	EXP_Default_Values.tpoc_date1,
	EXP_Default_Values.tpoc_amount1,
	EXP_Default_Values.tpoc_fund_dlay_dt1,
	-- *INF*: iif(isnull(lkp_claim_med_plan_tpoc_ak_id),'NEW',	
	-- 	iif (
	-- lkp_tpoc_fund_delay_date != tpoc_fund_dlay_dt1 OR
	-- lkp_tpoc_date != tpoc_date1 OR
	-- lkp_tpoc_amt != tpoc_amount1
	-- , 'UPDATE','NOCHANGE'))
	IFF(lkp_claim_med_plan_tpoc_ak_id IS NULL, 'NEW', IFF(lkp_tpoc_fund_delay_date != tpoc_fund_dlay_dt1 OR lkp_tpoc_date != tpoc_date1 OR lkp_tpoc_amt != tpoc_amount1, 'UPDATE', 'NOCHANGE')) AS v_Changed_Flag,
	1 AS Crrnt_Snpsht_Flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS Audit_Id,
	-- *INF*: IIF(v_Changed_Flag='NEW',
	-- 	TO_DATE('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'),
	-- 	SYSDATE)
	IFF(v_Changed_Flag = 'NEW', TO_DATE('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'), SYSDATE) AS Eff_From_Date,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS Eff_To_Date,
	v_Changed_Flag AS Changed_Flag,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS SOURCE_SYSTEM_ID,
	SYSDATE AS Created_Date,
	SYSDATE AS Modified_Date
	FROM EXP_Default_Values
	LEFT JOIN LKP_Target
	ON LKP_Target.claim_med_plan_ak_id = EXP_Default_Values.claim_med_plan_ak_id AND LKP_Target.tpoc_code = EXP_Default_Values.tpoc_code
),
FIL_Insert AS (
	SELECT
	Crrnt_Snpsht_Flag, 
	Audit_Id, 
	Eff_From_Date, 
	Eff_To_Date, 
	Changed_Flag, 
	SOURCE_SYSTEM_ID, 
	Created_Date, 
	Modified_Date, 
	lkp_claim_med_plan_tpoc_ak_id, 
	claim_med_plan_ak_id, 
	tpoc_code1, 
	tpoc_date1, 
	tpoc_amount1, 
	tpoc_fund_dlay_dt1
	FROM EXP_Detect_Changes
	WHERE Changed_Flag='NEW'  OR Changed_Flag='UPDATE'
),
SEQ_claim_medical_plan_tpoc AS (
	CREATE SEQUENCE SEQ_claim_medical_plan_tpoc
	START = 0
	INCREMENT = 1;
),
EXP_Insert AS (
	SELECT
	lkp_claim_med_plan_tpoc_ak_id,
	Crrnt_Snpsht_Flag,
	Audit_Id,
	Eff_From_Date,
	Eff_To_Date,
	Changed_Flag,
	SOURCE_SYSTEM_ID,
	Created_Date,
	Modified_Date,
	SEQ_claim_medical_plan_tpoc.NEXTVAL,
	-- *INF*: IIF(Changed_Flag='NEW', NEXTVAL, lkp_claim_med_plan_tpoc_ak_id)
	IFF(Changed_Flag = 'NEW', NEXTVAL, lkp_claim_med_plan_tpoc_ak_id) AS claim_med_plan_tpoc_ak_id_out,
	claim_med_plan_ak_id,
	tpoc_code1,
	tpoc_date1,
	tpoc_amount1,
	tpoc_fund_dlay_dt1
	FROM FIL_Insert
),
claim_medical_plan_tpoc_INSERT AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_medical_plan_tpoc
	(crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date, claim_med_plan_tpoc_ak_id, claim_med_plan_ak_id, tpoc_code, tpoc_fund_delay_date, tpoc_date, tpoc_amt)
	SELECT 
	Crrnt_Snpsht_Flag AS CRRNT_SNPSHT_FLAG, 
	Audit_Id AS AUDIT_ID, 
	Eff_From_Date AS EFF_FROM_DATE, 
	Eff_To_Date AS EFF_TO_DATE, 
	SOURCE_SYSTEM_ID AS SOURCE_SYS_ID, 
	Created_Date AS CREATED_DATE, 
	Modified_Date AS MODIFIED_DATE, 
	claim_med_plan_tpoc_ak_id_out AS CLAIM_MED_PLAN_TPOC_AK_ID, 
	CLAIM_MED_PLAN_AK_ID, 
	tpoc_code1 AS TPOC_CODE, 
	tpoc_fund_dlay_dt1 AS TPOC_FUND_DELAY_DATE, 
	tpoc_date1 AS TPOC_DATE, 
	tpoc_amount1 AS TPOC_AMT
	FROM EXP_Insert
),
SQ_claim_medical_plan_tpoc_REFRESH AS (
	SELECT
		claim_med_plan_tpoc_id,
		crrnt_snpsht_flag,
		audit_id,
		eff_from_date,
		eff_to_date,
		source_sys_id,
		created_date,
		modified_date,
		claim_med_plan_tpoc_ak_id,
		claim_med_plan_ak_id,
		tpoc_code,
		tpoc_fund_delay_date,
		tpoc_date,
		tpoc_amt
	FROM claim_medical_plan_tpoc_REFRESH
	WHERE claim_medical_plan_tpoc.crrnt_snpsht_flag='1'
),
EXP_claim_medical_plan_input_REFRESH AS (
	SELECT
	claim_med_plan_tpoc_id,
	crrnt_snpsht_flag,
	audit_id,
	eff_from_date,
	eff_to_date,
	source_sys_id,
	created_date,
	modified_date,
	claim_med_plan_tpoc_ak_id,
	claim_med_plan_ak_id,
	tpoc_code,
	tpoc_fund_delay_date,
	tpoc_date,
	tpoc_amt
	FROM SQ_claim_medical_plan_tpoc_REFRESH
),
JNR_refresh AS (SELECT
	EXP_claim_medical_plan_input_REFRESH.claim_med_plan_tpoc_id, 
	EXP_claim_medical_plan_input_REFRESH.crrnt_snpsht_flag, 
	EXP_claim_medical_plan_input_REFRESH.audit_id, 
	EXP_claim_medical_plan_input_REFRESH.eff_from_date, 
	EXP_claim_medical_plan_input_REFRESH.eff_to_date, 
	EXP_claim_medical_plan_input_REFRESH.source_sys_id, 
	EXP_claim_medical_plan_input_REFRESH.created_date, 
	EXP_claim_medical_plan_input_REFRESH.modified_date, 
	EXP_claim_medical_plan_input_REFRESH.claim_med_plan_tpoc_ak_id, 
	EXP_claim_medical_plan_input_REFRESH.claim_med_plan_ak_id, 
	EXP_claim_medical_plan_input_REFRESH.tpoc_code, 
	EXP_claim_medical_plan_input_REFRESH.tpoc_fund_delay_date, 
	EXP_claim_medical_plan_input_REFRESH.tpoc_date, 
	EXP_claim_medical_plan_input_REFRESH.tpoc_amt, 
	EXP_Default_Values.claim_med_plan_ak_id AS claim_med_plan_ak_id1, 
	EXP_Default_Values.tpoc_code AS tpoc_code1, 
	EXP_Default_Values.tpoc_date1, 
	EXP_Default_Values.tpoc_amount1 AS tpoc_amount, 
	EXP_Default_Values.tpoc_fund_dlay_dt1 AS tpoc_fund_dlay_dt
	FROM EXP_Default_Values
	RIGHT OUTER JOIN EXP_claim_medical_plan_input_REFRESH
	ON EXP_claim_medical_plan_input_REFRESH.claim_med_plan_ak_id = EXP_Default_Values.claim_med_plan_ak_id AND EXP_claim_medical_plan_input_REFRESH.tpoc_code = EXP_Default_Values.tpoc_code AND EXP_claim_medical_plan_input_REFRESH.tpoc_date = EXP_Default_Values.tpoc_date1 AND EXP_claim_medical_plan_input_REFRESH.tpoc_amt = EXP_Default_Values.tpoc_amount1 AND EXP_claim_medical_plan_input_REFRESH.tpoc_fund_delay_date = EXP_Default_Values.tpoc_fund_dlay_dt1
),
FIL_remove_matching_records AS (
	SELECT
	claim_med_plan_tpoc_id, 
	crrnt_snpsht_flag, 
	audit_id, 
	eff_from_date, 
	eff_to_date, 
	source_sys_id, 
	created_date, 
	modified_date, 
	claim_med_plan_tpoc_ak_id, 
	claim_med_plan_ak_id, 
	tpoc_code, 
	tpoc_fund_delay_date, 
	tpoc_date, 
	tpoc_amt, 
	claim_med_plan_ak_id1, 
	tpoc_code1, 
	tpoc_date1, 
	tpoc_amount, 
	tpoc_fund_dlay_dt
	FROM JNR_refresh
	WHERE ISNULL(claim_med_plan_ak_id1)
),
EXP_set_expire_values AS (
	SELECT
	claim_med_plan_tpoc_id,
	'0' AS current_snapshot_flag,
	SYSDATE AS mod_date
	FROM FIL_remove_matching_records
),
UPD_mark_for_update AS (
	SELECT
	claim_med_plan_tpoc_id, 
	current_snapshot_flag, 
	mod_date
	FROM EXP_set_expire_values
),
claim_medical_plan_tpoc_REFRESH_TARGET AS (
	MERGE INTO claim_medical_plan_tpoc AS T
	USING UPD_mark_for_update AS S
	ON T.claim_med_plan_tpoc_id = S.claim_med_plan_tpoc_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.current_snapshot_flag, T.modified_date = S.mod_date
),
SQ_claim_medical_plan_tpoc AS (
	SELECT 
	a.claim_med_plan_tpoc_id, 
	a.eff_from_date, 
	a.eff_to_date, 
	a.claim_med_plan_tpoc_ak_id 
	FROM
	 @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_medical_plan_tpoc a
	WHERE 
	 a.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' AND  
	 EXISTS(SELECT 1			
			FROM  @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_medical_plan_tpoc b
			WHERE source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'  AND crrnt_snpsht_flag = 1
			AND a.claim_med_plan_tpoc_ak_id  = b.claim_med_plan_tpoc_ak_id
			GROUP BY claim_med_plan_tpoc_ak_id
			HAVING COUNT(*) > 1)
	ORDER BY claim_med_plan_tpoc_ak_id, eff_from_date  DESC
),
EXP_Lag_eff_from_date AS (
	SELECT
	claim_med_plan_tpoc_id,
	claim_med_plan_tpoc_ak_id,
	eff_from_date,
	eff_to_date AS orig_eff_to_date,
	-- *INF*: DECODE(TRUE,
	-- 	claim_med_plan_tpoc_ak_id = v_PREV_ROW_claim_med_plan_tpoc_ak_id, ADD_TO_DATE(v_PREV_ROW_eff_from_date,'SS',-1),
	-- 	orig_eff_to_date)
	DECODE(TRUE,
		claim_med_plan_tpoc_ak_id = v_PREV_ROW_claim_med_plan_tpoc_ak_id, ADD_TO_DATE(v_PREV_ROW_eff_from_date, 'SS', - 1),
		orig_eff_to_date) AS v_eff_to_date,
	v_eff_to_date AS eff_to_date,
	eff_from_date AS v_PREV_ROW_eff_from_date,
	claim_med_plan_tpoc_ak_id AS v_PREV_ROW_claim_med_plan_tpoc_ak_id,
	SYSDATE AS modified_date,
	0 AS crrnt_snpsht_flag
	FROM SQ_claim_medical_plan_tpoc
),
FIL_FirstRowInAKGroup AS (
	SELECT
	claim_med_plan_tpoc_id, 
	orig_eff_to_date, 
	eff_to_date, 
	modified_date, 
	crrnt_snpsht_flag
	FROM EXP_Lag_eff_from_date
	WHERE orig_eff_to_date <> eff_to_date

--If these two dates equal each other we are dealing with the first row in an AK group.  This row
--does not need to be expired or updated for any reason thus it can be filtered out
-- but we must source it to capture the eff_from_date of this row 
--so that we can properly expire the subsequent row
),
UPD_Claim_Occurrence AS (
	SELECT
	claim_med_plan_tpoc_id, 
	eff_to_date, 
	modified_date, 
	crrnt_snpsht_flag
	FROM FIL_FirstRowInAKGroup
),
claim_medical_plan_tpoc_UPDATE AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_medical_plan_tpoc AS T
	USING UPD_Claim_Occurrence AS S
	ON T.claim_med_plan_tpoc_id = S.claim_med_plan_tpoc_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snpsht_flag, T.eff_to_date = S.eff_to_date, T.modified_date = S.modified_date
),