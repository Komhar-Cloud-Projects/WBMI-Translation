WITH
SQ_claimant_dim AS (
	SELECT 
	CLAIMANT_DIM.CLAIMANT_DIM_ID, 
	CLAIMANT_DIM.EDW_CLAIM_PARTY_OCCURRENCE_AK_ID ,
	CLAIMANT_DIM.EFF_FROM_DATE, 
	CLAIMANT_DIM.EFF_TO_DATE
	
	FROM
	DBO.CLAIMANT_DIM CLAIMANT_DIM
	
	WHERE 
	EFF_FROM_DATE <> EFF_TO_DATE AND 
	EDW_CLAIM_PARTY_OCCURRENCE_AK_ID IN
	
	(SELECT EDW_CLAIM_PARTY_OCCURRENCE_AK_ID 
	FROM DBO.CLAIMANT_DIM CLAIMANT_DIM2
	WHERE CRRNT_SNPSHT_FLAG = 1  
	GROUP BY CLAIMANT_DIM2.EDW_CLAIM_PARTY_OCCURRENCE_AK_ID HAVING COUNT(*) > 1)
	
	ORDER BY CLAIMANT_DIM.EDW_CLAIM_PARTY_OCCURRENCE_AK_ID,CLAIMANT_DIM.EFF_FROM_DATE DESC,
	CLAIMANT_DIM.edw_wc_claimant_work_hist_pk_id DESC
),
EXP_Lag_eff_from_date1 AS (
	SELECT
	claimant_dim_id,
	edw_claim_party_occurrence_ak_id,
	eff_from_date,
	eff_to_date AS orig_eff_to_date,
	-- *INF*: DECODE(TRUE,
	-- 	edw_claim_party_occurrence_ak_id = v_PREV_ROW_occurrence_key, ADD_TO_DATE(v_PREV_ROW_eff_from_date,'SS',-1),
	-- 	orig_eff_to_date)
	DECODE(
	    TRUE,
	    edw_claim_party_occurrence_ak_id = v_PREV_ROW_occurrence_key, DATEADD(SECOND,- 1,v_PREV_ROW_eff_from_date),
	    orig_eff_to_date
	) AS v_eff_to_date,
	v_eff_to_date AS eff_to_date,
	eff_from_date AS v_PREV_ROW_eff_from_date,
	edw_claim_party_occurrence_ak_id AS v_PREV_ROW_occurrence_key,
	SYSDATE AS modified_date,
	0 AS crrnt_snpsht_flag
	FROM SQ_claimant_dim
),
FIL_rows AS (
	SELECT
	claimant_dim_id, 
	orig_eff_to_date, 
	eff_to_date, 
	modified_date, 
	crrnt_snpsht_flag
	FROM EXP_Lag_eff_from_date1
	WHERE orig_eff_to_date <> eff_to_date
),
UPD_EFF_TO_DATE AS (
	SELECT
	claimant_dim_id, 
	eff_to_date, 
	modified_date, 
	crrnt_snpsht_flag
	FROM FIL_rows
),
claimant_dim_expire AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.claimant_dim AS T
	USING UPD_EFF_TO_DATE AS S
	ON T.claimant_dim_id = S.claimant_dim_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snpsht_flag, T.eff_to_date = S.eff_to_date, T.modified_date = S.modified_date
),
SQ_claimant_dim_Update_Claimant_Num AS (
	SELECT 
	CD.claimant_dim_id as claimant_dim_id,
	CPO.claim_party_occurrence_id as edw_claim_party_occurrence_pk_id ,
	CPO.claimant_num as claimant_gndr,
	CD.claimant_num as claimant_num
	FROM 
	@{pipeline().parameters.SOURCE_DATABASE}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_party_occurrence CPO, 
	@{pipeline().parameters.TARGET_DATABASE}.@{pipeline().parameters.TARGET_TABLE_OWNER}.claimant_dim CD
	WHERE 
	CD.edw_claim_party_occurrence_pk_id = CPO.claim_party_occurrence_id
	AND CPO.crrnt_snpsht_flag=1
	--AND CD.crrnt_snpsht_flag=1
	AND CPO.claimant_num <> CD.claimant_num
	AND CPO.source_sys_id='EXCEED'
),
EXP_Values AS (
	SELECT
	claimant_dim_id,
	edw_claim_party_occurrence_pk_id,
	claimant_gndr AS edw_claimant_num,
	claimant_num
	FROM SQ_claimant_dim_Update_Claimant_Num
),
UPD_Update_Claimant_Num AS (
	SELECT
	claimant_dim_id, 
	edw_claimant_num
	FROM EXP_Values
),
claimant_dim_Update_Claimant_Num AS (
	MERGE INTO claimant_dim AS T
	USING UPD_Update_Claimant_Num AS S
	ON T.claimant_dim_id = S.claimant_dim_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.claimant_num = S.edw_claimant_num
),
SQ_claimant_dim_Update_claim_medical_fields AS (
	SELECT 
	CD.claimant_dim_id as claimant_dim_id,
	CM.claim_med_id as edw_claim_med_pk_id,
	CM.medicare_eligibility as medicare_eligibility
	FROM 
	@{pipeline().parameters.TARGET_DATABASE}.@{pipeline().parameters.TARGET_TABLE_OWNER}.claimant_dim CD,
	@{pipeline().parameters.SOURCE_DATABASE}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_medical CM
	WHERE 
	CD.edw_claim_party_occurrence_ak_id = CM.claim_party_occurrence_ak_id
	AND CM.crrnt_snpsht_flag=1
	AND CD.crrnt_snpsht_flag=1
	AND CD.medicare_eligibility <> CM.medicare_eligibility
	AND CD.edw_claim_med_pk_id <> CM.claim_med_id
),
EXPTRANS AS (
	SELECT
	claimant_dim_id,
	edw_claim_med_pk_id,
	medicare_eligibility
	FROM SQ_claimant_dim_Update_claim_medical_fields
),
UPDTRANS AS (
	SELECT
	claimant_dim_id, 
	edw_claim_med_pk_id, 
	medicare_eligibility
	FROM EXPTRANS
),
claimant_dim_UPDate_claim_medical_Target AS (
	MERGE INTO claimant_dim AS T
	USING UPDTRANS AS S
	ON T.claimant_dim_id = S.claimant_dim_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.edw_claim_med_pk_id = S.edw_claim_med_pk_id, T.medicare_eligibility = S.medicare_eligibility
),