WITH
SEQ_claim_survey AS (
	CREATE SEQUENCE SEQ_claim_survey
	START = 0
	INCREMENT = 1;
),
SQ_claims_survey_result_stage AS (
	SELECT
		claims_survey_result_stage_id,
		form_num,
		claim_rep_code,
		ques1_resp_val,
		ques2_resp_val,
		ques3_resp_val,
		ques4_resp_val,
		ques5_resp_val,
		ques6_resp_val,
		ques7_resp_val,
		ques8_resp_val,
		ques9_resp_val,
		ques10_resp_val,
		ques11_resp_val,
		ques12_resp_val,
		ques13_resp_val,
		ques14_resp_val,
		ques15_resp_val,
		ques16_resp_val,
		ques17_resp_val,
		ques18_resp_val,
		entry_date,
		claim_num,
		extract_date,
		source_system_id
	FROM claims_survey_result_stage
),
lkp_claim_representative_occurrence AS (
	SELECT
	claim_rep_occurrence_ak_id,
	s3p_claim_num
	FROM (
		SELECT	REPOCC.claim_rep_occurrence_ak_id		AS claim_rep_occurrence_ak_id
		,		ltrim(rtrim(OCC.s3p_claim_num))			AS s3p_claim_num
		FROM	claim_occurrence	OCC
		INNER	JOIN claim_representative_occurrence	REPOCC
			ON	OCC.claim_occurrence_ak_id	= REPOCC.claim_occurrence_ak_id
		WHERE	REPOCC.claim_rep_role_code = 'H'
		AND		REPOCC.crrnt_snpsht_flag = 1
		AND		OCC.crrnt_snpsht_flag = 1
		ORDER	BY OCC.s3p_claim_num --
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY s3p_claim_num ORDER BY claim_rep_occurrence_ak_id) = 1
),
EXP_Detect_Changes AS (
	SELECT
	1 AS Crrnt_Snpsht_Flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS Audit_Id,
	-- *INF*: TO_DATE('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS') AS Eff_From_Date,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS Eff_To_Date,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS SOURCE_SYSTEM_ID,
	SYSDATE AS Created_Date,
	SYSDATE AS Modified_Date,
	SEQ_claim_survey.NEXTVAL AS claims_survey_result_ak_id,
	lkp_claim_representative_occurrence.claim_rep_occurrence_ak_id AS lkp_claim_rep_occurrence_ak_id,
	-- *INF*: iif(isnull(lkp_claim_rep_occurrence_ak_id),-1,lkp_claim_rep_occurrence_ak_id)
	IFF(lkp_claim_rep_occurrence_ak_id IS NULL, - 1, lkp_claim_rep_occurrence_ak_id) AS claim_rep_occurrence_ak_id,
	SQ_claims_survey_result_stage.form_num,
	SQ_claims_survey_result_stage.entry_date,
	SQ_claims_survey_result_stage.ques1_resp_val AS in_ques1_resp_val,
	-- *INF*: iif(isnull(in_ques1_resp_val),0,in_ques1_resp_val)
	IFF(in_ques1_resp_val IS NULL, 0, in_ques1_resp_val) AS ques1_resp_val,
	SQ_claims_survey_result_stage.ques2_resp_val AS in_ques2_resp_val,
	-- *INF*: iif(isnull(in_ques2_resp_val),0,in_ques2_resp_val)
	IFF(in_ques2_resp_val IS NULL, 0, in_ques2_resp_val) AS ques2_resp_val,
	SQ_claims_survey_result_stage.ques3_resp_val AS in_ques3_resp_val,
	-- *INF*: iif(isnull(in_ques3_resp_val),0,in_ques3_resp_val)
	IFF(in_ques3_resp_val IS NULL, 0, in_ques3_resp_val) AS ques3_resp_val,
	SQ_claims_survey_result_stage.ques4_resp_val AS in_ques4_resp_val,
	-- *INF*: iif(isnull(in_ques4_resp_val),0,in_ques4_resp_val)
	IFF(in_ques4_resp_val IS NULL, 0, in_ques4_resp_val) AS ques4_resp_val,
	SQ_claims_survey_result_stage.ques5_resp_val AS in_ques5_resp_val,
	-- *INF*: iif(isnull(in_ques5_resp_val),0,in_ques5_resp_val)
	IFF(in_ques5_resp_val IS NULL, 0, in_ques5_resp_val) AS ques5_resp_val,
	SQ_claims_survey_result_stage.ques6_resp_val AS in_ques6_resp_val,
	-- *INF*: iif(isnull(in_ques6_resp_val),0,in_ques6_resp_val)
	IFF(in_ques6_resp_val IS NULL, 0, in_ques6_resp_val) AS ques6_resp_val,
	SQ_claims_survey_result_stage.ques7_resp_val AS in_ques7_resp_val,
	-- *INF*: iif(isnull(in_ques7_resp_val),0,in_ques7_resp_val)
	IFF(in_ques7_resp_val IS NULL, 0, in_ques7_resp_val) AS ques7_resp_val,
	SQ_claims_survey_result_stage.ques8_resp_val AS in_ques8_resp_val,
	-- *INF*: iif(isnull(in_ques8_resp_val),0,in_ques8_resp_val)
	IFF(in_ques8_resp_val IS NULL, 0, in_ques8_resp_val) AS ques8_resp_val,
	SQ_claims_survey_result_stage.ques9_resp_val AS in_ques9_resp_val,
	-- *INF*: iif(isnull(in_ques9_resp_val),0,in_ques9_resp_val)
	IFF(in_ques9_resp_val IS NULL, 0, in_ques9_resp_val) AS ques9_resp_val,
	SQ_claims_survey_result_stage.ques10_resp_val AS in_ques10_resp_val,
	-- *INF*: iif(isnull(in_ques10_resp_val),0,in_ques10_resp_val)
	IFF(in_ques10_resp_val IS NULL, 0, in_ques10_resp_val) AS ques10_resp_val,
	SQ_claims_survey_result_stage.ques11_resp_val AS in_ques11_resp_val,
	-- *INF*: iif(isnull(in_ques11_resp_val),0,in_ques11_resp_val)
	IFF(in_ques11_resp_val IS NULL, 0, in_ques11_resp_val) AS ques11_resp_val,
	SQ_claims_survey_result_stage.ques12_resp_val AS in_ques12_resp_val,
	-- *INF*: iif(isnull(in_ques12_resp_val),0,in_ques12_resp_val)
	IFF(in_ques12_resp_val IS NULL, 0, in_ques12_resp_val) AS ques12_resp_val,
	SQ_claims_survey_result_stage.ques13_resp_val AS in_ques13_resp_val,
	-- *INF*: iif(isnull(in_ques13_resp_val),0,in_ques13_resp_val)
	IFF(in_ques13_resp_val IS NULL, 0, in_ques13_resp_val) AS ques13_resp_val,
	SQ_claims_survey_result_stage.ques14_resp_val AS in_ques14_resp_val,
	-- *INF*: iif(isnull(in_ques14_resp_val),0,in_ques14_resp_val)
	IFF(in_ques14_resp_val IS NULL, 0, in_ques14_resp_val) AS ques14_resp_val,
	SQ_claims_survey_result_stage.ques15_resp_val AS in_ques15_resp_val,
	-- *INF*: iif(isnull(in_ques15_resp_val),0,in_ques15_resp_val)
	IFF(in_ques15_resp_val IS NULL, 0, in_ques15_resp_val) AS ques15_resp_val,
	SQ_claims_survey_result_stage.ques16_resp_val AS in_ques16_resp_val,
	-- *INF*: iif(isnull(in_ques16_resp_val),0,in_ques16_resp_val)
	IFF(in_ques16_resp_val IS NULL, 0, in_ques16_resp_val) AS ques16_resp_val,
	SQ_claims_survey_result_stage.ques17_resp_val AS in_ques17_resp_val,
	-- *INF*: iif(isnull(in_ques17_resp_val),0,in_ques17_resp_val)
	IFF(in_ques17_resp_val IS NULL, 0, in_ques17_resp_val) AS ques17_resp_val,
	SQ_claims_survey_result_stage.ques18_resp_val AS in_ques18_resp_val,
	-- *INF*: iif(isnull(in_ques18_resp_val),0,in_ques18_resp_val)
	IFF(in_ques18_resp_val IS NULL, 0, in_ques18_resp_val) AS ques18_resp_val
	FROM SQ_claims_survey_result_stage
	LEFT JOIN lkp_claim_representative_occurrence
	ON lkp_claim_representative_occurrence.s3p_claim_num = SQ_claims_survey_result_stage.claim_num
),
claims_survey_result AS (
	INSERT INTO claims_survey_result
	(crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date, claims_survey_result_ak_id, claim_rep_occurrence_ak_id, claims_survey_form_num, entry_date, ques1_resp_val, ques2_resp_val, ques3_resp_val, ques4_resp_val, ques5_resp_val, ques6_resp_val, ques7_resp_val, ques8_resp_val, ques9_resp_val, ques10_resp_val, ques11_resp_val, ques12_resp_val, ques13_resp_val, ques14_resp_val, ques15_resp_val, ques16_resp_val, ques17_resp_val, ques18_resp_val)
	SELECT 
	Crrnt_Snpsht_Flag AS CRRNT_SNPSHT_FLAG, 
	Audit_Id AS AUDIT_ID, 
	Eff_From_Date AS EFF_FROM_DATE, 
	Eff_To_Date AS EFF_TO_DATE, 
	SOURCE_SYSTEM_ID AS SOURCE_SYS_ID, 
	Created_Date AS CREATED_DATE, 
	Modified_Date AS MODIFIED_DATE, 
	CLAIMS_SURVEY_RESULT_AK_ID, 
	CLAIM_REP_OCCURRENCE_AK_ID, 
	form_num AS CLAIMS_SURVEY_FORM_NUM, 
	ENTRY_DATE, 
	QUES1_RESP_VAL, 
	QUES2_RESP_VAL, 
	QUES3_RESP_VAL, 
	QUES4_RESP_VAL, 
	QUES5_RESP_VAL, 
	QUES6_RESP_VAL, 
	QUES7_RESP_VAL, 
	QUES8_RESP_VAL, 
	QUES9_RESP_VAL, 
	QUES10_RESP_VAL, 
	QUES11_RESP_VAL, 
	QUES12_RESP_VAL, 
	QUES13_RESP_VAL, 
	QUES14_RESP_VAL, 
	QUES15_RESP_VAL, 
	QUES16_RESP_VAL, 
	QUES17_RESP_VAL, 
	QUES18_RESP_VAL
	FROM EXP_Detect_Changes
),