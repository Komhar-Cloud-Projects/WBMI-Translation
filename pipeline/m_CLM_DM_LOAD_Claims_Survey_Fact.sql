WITH
SQ_claims_survey_result AS (
	SELECT Survey.claims_survey_result_id
	,		Survey.claims_survey_result_ak_id
	,		Survey.claims_survey_form_num
	,		Survey.entry_date
	,		Survey.ques1_resp_val
	,		Survey.ques2_resp_val
	,		Survey.ques3_resp_val
	,		Survey.ques4_resp_val
	,		Survey.ques5_resp_val
	,		Survey.ques6_resp_val
	,		Survey.ques7_resp_val
	,		Survey.ques8_resp_val
	,		Survey.ques9_resp_val
	,		Survey.ques10_resp_val
	,		Survey.ques11_resp_val
	,		Survey.ques12_resp_val
	,		Survey.ques13_resp_val
	,		Survey.ques14_resp_val
	,		Survey.ques15_resp_val
	,		Survey.ques16_resp_val
	,		Survey.ques17_resp_val
	,		Survey.ques18_resp_val
	,		isnull(RepOcc.claim_occurrence_ak_id,-1)	AS claim_occurrence_ak_id
	,		isnull(RepOcc.claim_rep_ak_id,-1)			AS claim_rep_ak_id
	FROM	@{pipeline().parameters.SOURCE_TABLE_OWNER}.claims_survey_result Survey
	LEFT	OUTER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_representative_occurrence RepOcc
		ON	Survey.claim_rep_occurrence_ak_id			= RepOcc.claim_rep_occurrence_ak_id
	WHERE	RepOcc.crrnt_snpsht_flag = 1
),
LKP_calender_dim AS (
	SELECT
	clndr_id,
	clndr_date
	FROM (
		SELECT 
			clndr_id,
			clndr_date
		FROM calendar_dim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY clndr_date ORDER BY clndr_id) = 1
),
LKP_claim_occurrence_dim AS (
	SELECT
	claim_occurrence_dim_id,
	edw_claim_occurrence_ak_id,
	eff_from_date,
	eff_to_date
	FROM (
		SELECT 
			claim_occurrence_dim_id,
			edw_claim_occurrence_ak_id,
			eff_from_date,
			eff_to_date
		FROM claim_occurrence_dim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_claim_occurrence_ak_id,eff_from_date,eff_to_date ORDER BY claim_occurrence_dim_id DESC) = 1
),
LKP_claim_representative_dim AS (
	SELECT
	claim_rep_dim_id,
	edw_claim_rep_ak_id,
	eff_from_date,
	eff_to_date
	FROM (
		SELECT 
			claim_rep_dim_id,
			edw_claim_rep_ak_id,
			eff_from_date,
			eff_to_date
		FROM claim_representative_dim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_claim_rep_ak_id,eff_from_date,eff_to_date ORDER BY claim_rep_dim_id DESC) = 1
),
lkp_claims_survey_form_dim AS (
	SELECT
	claims_survey_form_dim_id,
	claims_survey_form_num,
	eff_from_date,
	eff_to_date
	FROM (
		SELECT 
			claims_survey_form_dim_id,
			claims_survey_form_num,
			eff_from_date,
			eff_to_date
		FROM claims_survey_form_dim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claims_survey_form_num,eff_from_date,eff_to_date ORDER BY claims_survey_form_dim_id DESC) = 1
),
EXP_Values AS (
	SELECT
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	SQ_claims_survey_result.claims_survey_result_id AS edw_claims_survey_result_pk_id,
	SQ_claims_survey_result.claims_survey_result_ak_id AS edw_claims_survey_result_ak_id,
	lkp_claims_survey_form_dim.claims_survey_form_dim_id AS in_claims_survey_form_dim_id,
	-- *INF*: iif(isnull(in_claims_survey_form_dim_id),-1,in_claims_survey_form_dim_id)
	IFF(in_claims_survey_form_dim_id IS NULL, - 1, in_claims_survey_form_dim_id) AS claims_survey_form_dim_id,
	LKP_claim_occurrence_dim.claim_occurrence_dim_id AS in_claim_occurrence_dim_id,
	-- *INF*: iif(isnull(in_claim_occurrence_dim_id),-1,in_claim_occurrence_dim_id)
	IFF(in_claim_occurrence_dim_id IS NULL, - 1, in_claim_occurrence_dim_id) AS claim_occurrence_dim_id,
	LKP_claim_representative_dim.claim_rep_dim_id AS in_claim_rep_dim_id,
	-- *INF*: iif(isnull(in_claim_rep_dim_id),-1,in_claim_rep_dim_id)
	IFF(in_claim_rep_dim_id IS NULL, - 1, in_claim_rep_dim_id) AS claim_rep_dim_id,
	LKP_calender_dim.clndr_id AS in_entry_date_clndr_id,
	-- *INF*: iif(isnull(in_entry_date_clndr_id),-1,in_entry_date_clndr_id)
	IFF(in_entry_date_clndr_id IS NULL, - 1, in_entry_date_clndr_id) AS entry_date_clndr_id,
	SQ_claims_survey_result.ques1_resp_val,
	SQ_claims_survey_result.ques2_resp_val,
	SQ_claims_survey_result.ques3_resp_val,
	SQ_claims_survey_result.ques4_resp_val,
	SQ_claims_survey_result.ques5_resp_val,
	SQ_claims_survey_result.ques6_resp_val,
	SQ_claims_survey_result.ques7_resp_val,
	SQ_claims_survey_result.ques8_resp_val,
	SQ_claims_survey_result.ques9_resp_val,
	SQ_claims_survey_result.ques10_resp_val,
	SQ_claims_survey_result.ques11_resp_val,
	SQ_claims_survey_result.ques12_resp_val,
	SQ_claims_survey_result.ques13_resp_val,
	SQ_claims_survey_result.ques14_resp_val,
	SQ_claims_survey_result.ques15_resp_val,
	SQ_claims_survey_result.ques16_resp_val,
	SQ_claims_survey_result.ques17_resp_val,
	SQ_claims_survey_result.ques18_resp_val,
	-- *INF*: IIF(ques10_resp_val>3,1,0)
	IFF(ques10_resp_val > 3, 1, 0) AS raving_fan_flag,
	0 AS dtrctr_flag
	FROM SQ_claims_survey_result
	LEFT JOIN LKP_calender_dim
	ON LKP_calender_dim.clndr_date = SQ_claims_survey_result.entry_date
	LEFT JOIN LKP_claim_occurrence_dim
	ON LKP_claim_occurrence_dim.edw_claim_occurrence_ak_id = SQ_claims_survey_result.claim_occurrence_ak_id AND LKP_claim_occurrence_dim.eff_from_date <= SQ_claims_survey_result.entry_date AND LKP_claim_occurrence_dim.eff_to_date >= SQ_claims_survey_result.entry_date
	LEFT JOIN LKP_claim_representative_dim
	ON LKP_claim_representative_dim.edw_claim_rep_ak_id = SQ_claims_survey_result.claim_rep_ak_id AND LKP_claim_representative_dim.eff_from_date <= SQ_claims_survey_result.entry_date AND LKP_claim_representative_dim.eff_to_date >= SQ_claims_survey_result.entry_date
	LEFT JOIN lkp_claims_survey_form_dim
	ON lkp_claims_survey_form_dim.claims_survey_form_num = SQ_claims_survey_result.claims_survey_form_num AND lkp_claims_survey_form_dim.eff_from_date <= SQ_claims_survey_result.entry_date AND lkp_claims_survey_form_dim.eff_to_date >= SQ_claims_survey_result.entry_date
),
lkp_claims_survey_fact AS (
	SELECT
	in_edw_claims_survey_result_pk_id,
	claims_survey_fact_id,
	edw_claims_survey_result_pk_id,
	edw_claims_survey_result_ak_id,
	claims_survey_form_dim_id,
	claim_occurrence_dim_id,
	claim_rep_dim_id,
	entry_date_clndr_id,
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
	raving_fan_flag,
	dtrctr_flag
	FROM (
		SELECT 
			in_edw_claims_survey_result_pk_id,
			claims_survey_fact_id,
			edw_claims_survey_result_pk_id,
			edw_claims_survey_result_ak_id,
			claims_survey_form_dim_id,
			claim_occurrence_dim_id,
			claim_rep_dim_id,
			entry_date_clndr_id,
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
			raving_fan_flag,
			dtrctr_flag
		FROM claims_survey_fact
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_claims_survey_result_pk_id ORDER BY in_edw_claims_survey_result_pk_id DESC) = 1
),
EXP_Detect_Changes AS (
	SELECT
	lkp_claims_survey_fact.claims_survey_fact_id AS lkp_claims_survey_fact_id,
	lkp_claims_survey_fact.edw_claims_survey_result_pk_id AS lkp_edw_claims_survey_result_pk_id,
	lkp_claims_survey_fact.edw_claims_survey_result_ak_id AS lkp_edw_claims_survey_result_ak_id,
	lkp_claims_survey_fact.claims_survey_form_dim_id AS lkp_claims_survey_form_dim_id,
	lkp_claims_survey_fact.claim_occurrence_dim_id AS lkp_claim_occurrence_dim_id,
	lkp_claims_survey_fact.claim_rep_dim_id AS lkp_claim_rep_dim_id,
	lkp_claims_survey_fact.entry_date_clndr_id AS lkp_entry_date_clndr_id,
	lkp_claims_survey_fact.ques1_resp_val AS lkp_ques1_resp_val,
	lkp_claims_survey_fact.ques2_resp_val AS lkp_ques2_resp_val,
	lkp_claims_survey_fact.ques3_resp_val AS lkp_ques3_resp_val,
	lkp_claims_survey_fact.ques4_resp_val AS lkp_ques4_resp_val,
	lkp_claims_survey_fact.ques5_resp_val AS lkp_ques5_resp_val,
	lkp_claims_survey_fact.ques6_resp_val AS lkp_ques6_resp_val,
	lkp_claims_survey_fact.ques7_resp_val AS lkp_ques7_resp_val,
	lkp_claims_survey_fact.ques8_resp_val AS lkp_ques8_resp_val,
	lkp_claims_survey_fact.ques9_resp_val AS lkp_ques9_resp_val,
	lkp_claims_survey_fact.ques10_resp_val AS lkp_ques10_resp_val,
	lkp_claims_survey_fact.ques11_resp_val AS lkp_ques11_resp_val,
	lkp_claims_survey_fact.ques12_resp_val AS lkp_ques12_resp_val,
	lkp_claims_survey_fact.ques13_resp_val AS lkp_ques13_resp_val,
	lkp_claims_survey_fact.ques14_resp_val AS lkp_ques14_resp_val,
	lkp_claims_survey_fact.ques15_resp_val AS lkp_ques15_resp_val,
	lkp_claims_survey_fact.ques16_resp_val AS lkp_ques16_resp_val,
	lkp_claims_survey_fact.ques17_resp_val AS lkp_ques17_resp_val,
	lkp_claims_survey_fact.ques18_resp_val AS lkp_ques18_resp_val,
	lkp_claims_survey_fact.raving_fan_flag AS lkp_raving_fan_flag,
	lkp_claims_survey_fact.dtrctr_flag AS lkp_dtrctr_flag,
	EXP_Values.audit_id,
	EXP_Values.edw_claims_survey_result_pk_id,
	EXP_Values.edw_claims_survey_result_ak_id,
	EXP_Values.claims_survey_form_dim_id,
	EXP_Values.claim_occurrence_dim_id,
	EXP_Values.claim_rep_dim_id,
	EXP_Values.entry_date_clndr_id,
	EXP_Values.ques1_resp_val,
	EXP_Values.ques2_resp_val,
	EXP_Values.ques3_resp_val,
	EXP_Values.ques4_resp_val,
	EXP_Values.ques5_resp_val,
	EXP_Values.ques6_resp_val,
	EXP_Values.ques7_resp_val,
	EXP_Values.ques8_resp_val,
	EXP_Values.ques9_resp_val,
	EXP_Values.ques10_resp_val,
	EXP_Values.ques11_resp_val,
	EXP_Values.ques12_resp_val,
	EXP_Values.ques13_resp_val,
	EXP_Values.ques14_resp_val,
	EXP_Values.ques15_resp_val,
	EXP_Values.ques16_resp_val,
	EXP_Values.ques17_resp_val,
	EXP_Values.ques18_resp_val,
	EXP_Values.raving_fan_flag,
	EXP_Values.dtrctr_flag,
	-- *INF*: IIF(ISNULL(lkp_claims_survey_fact_id) ,'NEW',
	-- IIF(
	-- lkp_claims_survey_form_dim_id <> claims_survey_form_dim_id OR
	-- lkp_claim_occurrence_dim_id <> claim_occurrence_dim_id OR
	-- lkp_claim_rep_dim_id <> claim_rep_dim_id OR
	-- lkp_entry_date_clndr_id <> entry_date_clndr_id OR
	-- lkp_ques1_resp_val <> ques1_resp_val OR
	-- lkp_ques2_resp_val <> ques2_resp_val OR
	-- lkp_ques3_resp_val <> ques3_resp_val OR
	-- lkp_ques4_resp_val <> ques4_resp_val OR
	-- lkp_ques5_resp_val <> ques5_resp_val OR
	-- lkp_ques6_resp_val <> ques6_resp_val OR
	-- lkp_ques7_resp_val <> ques7_resp_val OR
	-- lkp_ques8_resp_val <> ques8_resp_val OR
	-- lkp_ques9_resp_val <> ques9_resp_val OR
	-- lkp_ques10_resp_val <> ques10_resp_val OR
	-- lkp_ques11_resp_val <> ques11_resp_val OR
	-- lkp_ques12_resp_val <> ques12_resp_val OR
	-- lkp_ques13_resp_val <> ques13_resp_val OR
	-- lkp_ques14_resp_val <> ques14_resp_val OR
	-- lkp_ques15_resp_val <> ques15_resp_val OR
	-- lkp_ques16_resp_val <> ques16_resp_val OR
	-- lkp_ques17_resp_val <> ques17_resp_val OR
	-- lkp_ques18_resp_val <> ques18_resp_val OR
	-- lkp_raving_fan_flag <> raving_fan_flag OR
	-- lkp_dtrctr_flag <> dtrctr_flag 
	-- ,
	-- 'UPDATE','NOCHANGE'))
	-- 
	IFF(
	    lkp_claims_survey_fact_id IS NULL, 'NEW',
	    IFF(
	        lkp_claims_survey_form_dim_id <> claims_survey_form_dim_id
	        or lkp_claim_occurrence_dim_id <> claim_occurrence_dim_id
	        or lkp_claim_rep_dim_id <> claim_rep_dim_id
	        or lkp_entry_date_clndr_id <> entry_date_clndr_id
	        or lkp_ques1_resp_val <> ques1_resp_val
	        or lkp_ques2_resp_val <> ques2_resp_val
	        or lkp_ques3_resp_val <> ques3_resp_val
	        or lkp_ques4_resp_val <> ques4_resp_val
	        or lkp_ques5_resp_val <> ques5_resp_val
	        or lkp_ques6_resp_val <> ques6_resp_val
	        or lkp_ques7_resp_val <> ques7_resp_val
	        or lkp_ques8_resp_val <> ques8_resp_val
	        or lkp_ques9_resp_val <> ques9_resp_val
	        or lkp_ques10_resp_val <> ques10_resp_val
	        or lkp_ques11_resp_val <> ques11_resp_val
	        or lkp_ques12_resp_val <> ques12_resp_val
	        or lkp_ques13_resp_val <> ques13_resp_val
	        or lkp_ques14_resp_val <> ques14_resp_val
	        or lkp_ques15_resp_val <> ques15_resp_val
	        or lkp_ques16_resp_val <> ques16_resp_val
	        or lkp_ques17_resp_val <> ques17_resp_val
	        or lkp_ques18_resp_val <> ques18_resp_val
	        or lkp_raving_fan_flag <> raving_fan_flag
	        or lkp_dtrctr_flag <> dtrctr_flag,
	        'UPDATE',
	        'NOCHANGE'
	    )
	) AS v_changed_flag,
	v_changed_flag AS changed_flag,
	SYSDATE AS modified_date
	FROM EXP_Values
	LEFT JOIN lkp_claims_survey_fact
	ON lkp_claims_survey_fact.edw_claims_survey_result_pk_id = EXP_Values.edw_claims_survey_result_pk_id
),
RTR_InsertUpdate AS (
	SELECT
	lkp_claims_survey_fact_id,
	audit_id,
	edw_claims_survey_result_pk_id,
	edw_claims_survey_result_ak_id,
	claims_survey_form_dim_id,
	claim_occurrence_dim_id,
	claim_rep_dim_id,
	entry_date_clndr_id,
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
	raving_fan_flag,
	dtrctr_flag,
	changed_flag
	FROM EXP_Detect_Changes
),
RTR_InsertUpdate_InsertRecords AS (SELECT * FROM RTR_InsertUpdate WHERE changed_flag = 'NEW'),
RTR_InsertUpdate_UpdateRecords AS (SELECT * FROM RTR_InsertUpdate WHERE changed_flag = 'UPDATE'),
UPD_claims_survey_fact_Update AS (
	SELECT
	lkp_claims_survey_fact_id, 
	edw_claims_survey_result_pk_id, 
	edw_claims_survey_result_ak_id, 
	claims_survey_form_dim_id, 
	claim_occurrence_dim_id, 
	claim_rep_dim_id, 
	entry_date_clndr_id, 
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
	raving_fan_flag, 
	dtrctr_flag
	FROM RTR_InsertUpdate_UpdateRecords
),
claims_survey_fact_update AS (
	MERGE INTO claims_survey_fact AS T
	USING UPD_claims_survey_fact_Update AS S
	ON T.claims_survey_fact_id = S.lkp_claims_survey_fact_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.edw_claims_survey_result_pk_id = S.edw_claims_survey_result_pk_id, T.edw_claims_survey_result_ak_id = S.edw_claims_survey_result_ak_id, T.claims_survey_form_dim_id = S.claims_survey_form_dim_id, T.claim_occurrence_dim_id = S.claim_occurrence_dim_id, T.claim_rep_dim_id = S.claim_rep_dim_id, T.entry_date_clndr_id = S.entry_date_clndr_id, T.ques1_resp_val = S.ques1_resp_val, T.ques2_resp_val = S.ques2_resp_val, T.ques3_resp_val = S.ques3_resp_val, T.ques4_resp_val = S.ques4_resp_val, T.ques5_resp_val = S.ques5_resp_val, T.ques6_resp_val = S.ques6_resp_val, T.ques7_resp_val = S.ques7_resp_val, T.ques8_resp_val = S.ques8_resp_val, T.ques9_resp_val = S.ques9_resp_val, T.ques10_resp_val = S.ques10_resp_val, T.ques11_resp_val = S.ques11_resp_val, T.ques12_resp_val = S.ques12_resp_val, T.ques13_resp_val = S.ques13_resp_val, T.ques14_resp_val = S.ques14_resp_val, T.ques15_resp_val = S.ques15_resp_val, T.ques16_resp_val = S.ques16_resp_val, T.ques17_resp_val = S.ques17_resp_val, T.ques18_resp_val = S.ques18_resp_val, T.raving_fan_flag = S.raving_fan_flag, T.dtrctr_flag = S.dtrctr_flag
),
UPD_claims_survey_fact_Insert AS (
	SELECT
	audit_id, 
	edw_claims_survey_result_pk_id, 
	edw_claims_survey_result_ak_id, 
	claims_survey_form_dim_id, 
	claim_occurrence_dim_id, 
	claim_rep_dim_id, 
	entry_date_clndr_id, 
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
	raving_fan_flag, 
	dtrctr_flag
	FROM RTR_InsertUpdate_InsertRecords
),
claims_survey_fact_insert AS (
	INSERT INTO claims_survey_fact
	(audit_id, edw_claims_survey_result_pk_id, edw_claims_survey_result_ak_id, claims_survey_form_dim_id, claim_occurrence_dim_id, claim_rep_dim_id, entry_date_clndr_id, ques1_resp_val, ques2_resp_val, ques3_resp_val, ques4_resp_val, ques5_resp_val, ques6_resp_val, ques7_resp_val, ques8_resp_val, ques9_resp_val, ques10_resp_val, ques11_resp_val, ques12_resp_val, ques13_resp_val, ques14_resp_val, ques15_resp_val, ques16_resp_val, ques17_resp_val, ques18_resp_val, raving_fan_flag, dtrctr_flag)
	SELECT 
	AUDIT_ID, 
	EDW_CLAIMS_SURVEY_RESULT_PK_ID, 
	EDW_CLAIMS_SURVEY_RESULT_AK_ID, 
	CLAIMS_SURVEY_FORM_DIM_ID, 
	CLAIM_OCCURRENCE_DIM_ID, 
	CLAIM_REP_DIM_ID, 
	ENTRY_DATE_CLNDR_ID, 
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
	QUES18_RESP_VAL, 
	RAVING_FAN_FLAG, 
	DTRCTR_FLAG
	FROM UPD_claims_survey_fact_Insert
),