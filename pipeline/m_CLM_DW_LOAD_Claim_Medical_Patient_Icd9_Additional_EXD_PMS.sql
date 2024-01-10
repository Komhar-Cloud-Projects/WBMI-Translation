WITH
SQ_claim_medical_stage AS (
	SELECT
		claim_medical_stage_id,
		injured_party_id,
		medicare_hicn,
		medicare_elig,
		cms_date_of_incid,
		cause_code,
		diag_code1,
		diag_code2,
		diag_code3,
		diag_code4,
		diag_code5,
		diag_code6,
		diag_code7,
		diag_code8,
		diag_code9,
		diag_code10,
		diag_code11,
		diag_code12,
		diag_code13,
		diag_code14,
		diag_code15,
		diag_code16,
		diag_code17,
		diag_code18,
		diag_code19,
		self_insd_ind,
		self_insd_type,
		self_insd_fst_nm,
		self_insd_last_nm,
		self_insd_dba_nm,
		self_insd_lgl_nm,
		product_liab_ind,
		prod_generic_nm,
		prod_brand_nm,
		prod_manufacturer,
		prod_allege_harm,
		inj_par_rep_firm,
		exceed_claim_key,
		exceed_claimnt_key,
		pms_policy_sym,
		pms_policy_num,
		pms_policy_mod,
		pms_date_of_loss,
		pms_loss_occurence,
		pms_loss_claimant,
		cms_source_system_id,
		clmt1_rep_firm,
		clmt2_rep_firm,
		clmt3_rep_firm,
		clmt4_rep_firm,
		created_ts,
		created_user_id,
		modified_ts,
		modified_user_id,
		extract_date,
		as_of_date,
		record_count,
		source_system_id,
		query_requested,
		query_request_date
	FROM claim_medical_stage
),
EXP_Values AS (
	SELECT
	exceed_claim_key,
	exceed_claimnt_key,
	pms_policy_sym,
	pms_policy_num,
	pms_policy_mod,
	pms_date_of_loss,
	pms_loss_occurence,
	pms_loss_claimant,
	cms_source_system_id,
	-- *INF*: replaceChr(0,to_char(pms_date_of_loss),'/','')
	replaceChr(0, to_char(pms_date_of_loss), '/', '') AS v_pms_date_of_loss,
	-- *INF*: IIF(length(exceed_claim_key)>0,ltrim(rtrim(exceed_claim_key)),
	-- IIF(length(pms_policy_sym)>0,ltrim(rtrim(pms_policy_sym || pms_policy_num || pms_policy_mod || v_pms_date_of_loss || pms_loss_occurence)),
	-- 'N/A'))
	-- 
	IFF(length(exceed_claim_key) > 0, ltrim(rtrim(exceed_claim_key)), IFF(length(pms_policy_sym) > 0, ltrim(rtrim(pms_policy_sym || pms_policy_num || pms_policy_mod || v_pms_date_of_loss || pms_loss_occurence)), 'N/A')) AS claim_key1,
	-- *INF*: IIF(length(exceed_claim_key)>0,ltrim(rtrim(exceed_claimnt_key)),
	-- IIF(length(pms_policy_sym)>0,ltrim(rtrim(pms_policy_sym || pms_policy_num || pms_policy_mod || v_pms_date_of_loss || pms_loss_occurence || pms_loss_claimant || 'CMT')),
	-- 'N/A'))
	IFF(length(exceed_claim_key) > 0, ltrim(rtrim(exceed_claimnt_key)), IFF(length(pms_policy_sym) > 0, ltrim(rtrim(pms_policy_sym || pms_policy_num || pms_policy_mod || v_pms_date_of_loss || pms_loss_occurence || pms_loss_claimant || 'CMT')), 'N/A')) AS claimnt_key1,
	diag_code2,
	diag_code3,
	diag_code4,
	diag_code5,
	diag_code6,
	diag_code7,
	diag_code8,
	diag_code9,
	diag_code10,
	diag_code11,
	diag_code12,
	diag_code13,
	diag_code14,
	diag_code15,
	diag_code16,
	diag_code17,
	diag_code18,
	diag_code19
	FROM SQ_claim_medical_stage
),
LKP_Claim_Med_AK_ID AS (
	SELECT
	claim_med_ak_id,
	claimant_num,
	claim_party_role_code
	FROM (
		SELECT 
		CM.claim_med_ak_id as claim_med_ak_id, 
		LTRIM(RTRIM(CO.claim_occurrence_key)) as claimant_num, 
		LTRIM(RTRIM(CP.claim_party_key)) as claim_party_role_code
		FROM 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.claim_medical CM,
		@{pipeline().parameters.TARGET_TABLE_OWNER}.claim_party_occurrence CPO,
		@{pipeline().parameters.TARGET_TABLE_OWNER}.claim_party CP, 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.claim_occurrence CO
		WHERE 
		CM.claim_party_occurrence_ak_id = CPO.claim_party_occurrence_ak_id
		AND CO.claim_occurrence_ak_id = CPO.claim_occurrence_ak_id  
		AND CP.claim_party_ak_id = CPO.claim_party_ak_id 
		AND CPO.claim_party_role_code IN ('CMT' , 'CLMT')
		AND CO.crrnt_snpsht_flag = 1
		AND CP.crrnt_snpsht_flag = 1
		AND CPO.crrnt_snpsht_flag = 1
		AND CM.crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claimant_num,claim_party_role_code ORDER BY claim_med_ak_id) = 1
),
NRM_Values AS (
),
EXP_Default_values AS (
	SELECT
	claim_med_ak_id,
	GCID_diag_code_val AS in_diag_code,
	in_diag_code + 1 AS diag_code,
	diag_code_val AS in_diag_code_val,
	-- *INF*: LTRIM(RTRIM(in_diag_code_val))
	LTRIM(RTRIM(in_diag_code_val)) AS diag_code_val
	FROM NRM_Values
),
FIL_Valid_Codes AS (
	SELECT
	claim_med_ak_id, 
	diag_code, 
	diag_code_val
	FROM EXP_Default_values
	WHERE IIF(ISNULL(diag_code_val),FALSE,TRUE)
),
LKP_Target AS (
	SELECT
	claim_med_patient_diag_add_ak_id,
	claim_med_ak_id,
	patient_add_code,
	patient_diag_code
	FROM (
		SELECT 
		a.claim_med_patient_diag_add_ak_id as claim_med_patient_diag_add_ak_id, 
		ltrim(rtrim(a.patient_diag_code)) as patient_diag_code, 
		a.claim_med_ak_id as claim_med_ak_id, 
		a.patient_add_code as patient_add_code 
		FROM 
			@{pipeline().parameters.TARGET_TABLE_OWNER}.claim_medical_patient_diagnosis_additional a
		WHERE
			crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_med_ak_id,patient_add_code ORDER BY claim_med_patient_diag_add_ak_id) = 1
),
EXP_Detect_Changes AS (
	SELECT
	FIL_Valid_Codes.claim_med_ak_id,
	FIL_Valid_Codes.diag_code,
	FIL_Valid_Codes.diag_code_val,
	LKP_Target.claim_med_patient_diag_add_ak_id,
	LKP_Target.patient_diag_code,
	-- *INF*: iif(isnull(claim_med_patient_diag_add_ak_id),'NEW',	
	-- 	iif (diag_code_val <> patient_diag_code,
	--  'UPDATE','NOCHANGE'))
	IFF(claim_med_patient_diag_add_ak_id IS NULL, 'NEW', IFF(diag_code_val <> patient_diag_code, 'UPDATE', 'NOCHANGE')) AS v_Changed_Flag,
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
	FROM FIL_Valid_Codes
	LEFT JOIN LKP_Target
	ON LKP_Target.claim_med_ak_id = FIL_Valid_Codes.claim_med_ak_id AND LKP_Target.patient_add_code = FIL_Valid_Codes.diag_code
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
	claim_med_ak_id, 
	diag_code, 
	diag_code_val, 
	claim_med_patient_diag_add_ak_id
	FROM EXP_Detect_Changes
	WHERE Changed_Flag='NEW'  OR Changed_Flag='UPDATE'
),
SEQ_claim_medical_patient_icd9_additional AS (
	CREATE SEQUENCE SEQ_claim_medical_patient_icd9_additional
	START = 0
	INCREMENT = 1;
),
EXP_Insert AS (
	SELECT
	claim_med_patient_diag_add_ak_id,
	Crrnt_Snpsht_Flag,
	Audit_Id,
	Eff_From_Date,
	Eff_To_Date,
	Changed_Flag,
	SOURCE_SYSTEM_ID,
	Created_Date,
	Modified_Date,
	SEQ_claim_medical_patient_icd9_additional.NEXTVAL,
	-- *INF*: IIF(Changed_Flag='NEW', NEXTVAL, claim_med_patient_diag_add_ak_id)
	IFF(Changed_Flag = 'NEW', NEXTVAL, claim_med_patient_diag_add_ak_id) AS claim_med_patient_diag_add_ak_id_out,
	claim_med_ak_id,
	diag_code,
	diag_code_val
	FROM FIL_Insert
),
claim_medical_patient_diagnosis_additional_Insert AS (
	INSERT INTO claim_medical_patient_diagnosis_additional
	(crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date, claim_med_patient_diag_add_ak_id, claim_med_ak_id, patient_add_code, patient_diag_code)
	SELECT 
	Crrnt_Snpsht_Flag AS CRRNT_SNPSHT_FLAG, 
	Audit_Id AS AUDIT_ID, 
	Eff_From_Date AS EFF_FROM_DATE, 
	Eff_To_Date AS EFF_TO_DATE, 
	SOURCE_SYSTEM_ID AS SOURCE_SYS_ID, 
	Created_Date AS CREATED_DATE, 
	Modified_Date AS MODIFIED_DATE, 
	claim_med_patient_diag_add_ak_id_out AS CLAIM_MED_PATIENT_DIAG_ADD_AK_ID, 
	CLAIM_MED_AK_ID, 
	diag_code AS PATIENT_ADD_CODE, 
	diag_code_val AS PATIENT_DIAG_CODE
	FROM EXP_Insert
),
SQ_claim_medical_patient_icd9_additional_REFRESH AS (
	SELECT
		claim_med_patient_diag_add_id,
		crrnt_snpsht_flag,
		audit_id,
		eff_from_date,
		eff_to_date,
		source_sys_id,
		created_date,
		modified_date,
		claim_med_patient_diag_add_ak_id,
		claim_med_ak_id,
		patient_add_code,
		patient_diag_code
	FROM claim_medical_patient_diagnosis_additional1
	WHERE crrnt_snpsht_flag='1'
),
EXP_input_refresh AS (
	SELECT
	claim_med_patient_diag_add_id,
	crrnt_snpsht_flag,
	audit_id,
	eff_from_date,
	eff_to_date,
	source_sys_id,
	created_date,
	modified_date,
	claim_med_patient_diag_add_ak_id,
	claim_med_ak_id,
	patient_add_code,
	patient_diag_code,
	-- *INF*: rtrim(ltrim(patient_diag_code))
	rtrim(ltrim(patient_diag_code)) AS patient_diag_code_val_out
	FROM SQ_claim_medical_patient_icd9_additional_REFRESH
),
JNR_refresh AS (SELECT
	EXP_input_refresh.claim_med_patient_diag_add_id, 
	EXP_input_refresh.crrnt_snpsht_flag, 
	EXP_input_refresh.audit_id, 
	EXP_input_refresh.eff_from_date, 
	EXP_input_refresh.eff_to_date, 
	EXP_input_refresh.source_sys_id, 
	EXP_input_refresh.created_date, 
	EXP_input_refresh.modified_date, 
	EXP_input_refresh.claim_med_patient_diag_add_ak_id, 
	EXP_input_refresh.claim_med_ak_id, 
	EXP_input_refresh.patient_add_code, 
	EXP_input_refresh.patient_diag_code_val_out, 
	EXP_Default_values.claim_med_ak_id AS claim_med_ak_id_insert, 
	EXP_Default_values.diag_code, 
	EXP_Default_values.diag_code_val
	FROM EXP_Default_values
	RIGHT OUTER JOIN EXP_input_refresh
	ON EXP_input_refresh.claim_med_ak_id = EXP_Default_values.claim_med_ak_id AND EXP_input_refresh.patient_add_code = EXP_Default_values.diag_code AND EXP_input_refresh.patient_diag_code_val_out = EXP_Default_values.diag_code_val
),
FIL_refresh AS (
	SELECT
	claim_med_patient_diag_add_id, 
	crrnt_snpsht_flag, 
	audit_id, 
	eff_from_date, 
	eff_to_date, 
	source_sys_id, 
	created_date, 
	modified_date, 
	claim_med_patient_diag_add_ak_id, 
	claim_med_ak_id, 
	patient_add_code, 
	patient_diag_code_val_out, 
	claim_med_ak_id_insert, 
	diag_code, 
	diag_code_val
	FROM JNR_refresh
	WHERE isnull(claim_med_ak_id_insert)
),
EXP_default_refresh AS (
	SELECT
	claim_med_patient_diag_add_id,
	'0' AS current_snapshot_flag,
	SYSDATE AS mod_date
	FROM FIL_refresh
),
UPD_refresh AS (
	SELECT
	claim_med_patient_diag_add_id, 
	current_snapshot_flag, 
	mod_date
	FROM EXP_default_refresh
),
claim_medical_patient_diagnosis_additional_REFRESH_Target AS (
	MERGE INTO claim_medical_patient_diagnosis_additional AS T
	USING UPD_refresh AS S
	ON T.claim_med_patient_diag_add_id = S.claim_med_patient_diag_add_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.current_snapshot_flag, T.modified_date = S.mod_date
),
SQ_claim_medical_patient_diagnosis_additional AS (
	SELECT 
	a.claim_med_patient_diag_add_id, 
	a.eff_from_date, 
	a.eff_to_date, 
	a.claim_med_patient_diag_add_ak_id 
	FROM
	 @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_medical_patient_diagnosis_additional a
	WHERE 
	 a.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' AND  
	 EXISTS(SELECT 1			
			FROM  @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_medical_patient_diagnosis_additional b
			WHERE source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'  AND crrnt_snpsht_flag = 1
			AND a.claim_med_patient_diag_add_ak_id  = b.claim_med_patient_diag_add_ak_id
			GROUP BY claim_med_patient_diag_add_ak_id 
			HAVING COUNT(*) > 1)
	ORDER BY claim_med_patient_diag_add_ak_id, eff_from_date  DESC
),
EXP_Lag_eff_from_date AS (
	SELECT
	claim_med_patient_diag_add_id,
	claim_med_patient_diag_add_ak_id,
	eff_from_date,
	eff_to_date AS orig_eff_to_date,
	-- *INF*: DECODE(TRUE,
	-- 	claim_med_patient_diag_add_ak_id = v_PREV_ROW_claim_med_patient_icd9_add_ak_id, ADD_TO_DATE(v_PREV_ROW_eff_from_date,'SS',-1),
	-- 	orig_eff_to_date)
	DECODE(TRUE,
		claim_med_patient_diag_add_ak_id = v_PREV_ROW_claim_med_patient_icd9_add_ak_id, ADD_TO_DATE(v_PREV_ROW_eff_from_date, 'SS', - 1),
		orig_eff_to_date) AS v_eff_to_date,
	v_eff_to_date AS eff_to_date,
	eff_from_date AS v_PREV_ROW_eff_from_date,
	claim_med_patient_diag_add_ak_id AS v_PREV_ROW_claim_med_patient_icd9_add_ak_id,
	SYSDATE AS modified_date,
	0 AS crrnt_snpsht_flag
	FROM SQ_claim_medical_patient_diagnosis_additional
),
FIL_FirstRowInAKGroup AS (
	SELECT
	claim_med_patient_diag_add_id, 
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
	claim_med_patient_diag_add_id, 
	eff_to_date, 
	modified_date, 
	crrnt_snpsht_flag
	FROM FIL_FirstRowInAKGroup
),
claim_medical_patient_diagnosis_additional_Update AS (
	MERGE INTO claim_medical_patient_diagnosis_additional AS T
	USING UPD_Claim_Occurrence AS S
	ON T.claim_med_patient_diag_add_id = S.claim_med_patient_diag_add_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snpsht_flag, T.eff_to_date = S.eff_to_date, T.modified_date = S.modified_date
),