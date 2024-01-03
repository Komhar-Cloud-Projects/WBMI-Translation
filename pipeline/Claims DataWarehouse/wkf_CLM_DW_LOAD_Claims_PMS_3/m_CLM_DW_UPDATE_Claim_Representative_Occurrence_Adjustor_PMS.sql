WITH
LKP_CLAIM_REP AS (
	SELECT
	claim_rep_ak_id,
	claim_rep_key
	FROM (
		SELECT 
			claim_rep_ak_id,
			claim_rep_key
		FROM claim_representative
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_rep_key ORDER BY claim_rep_ak_id) = 1
),
SQ_Claim_Representative_Occurrence_PMS_fix AS (
	SELECT claim_occurrence.claim_occurrence_ak_id, claim_occurrence.claim_occurrence_key, claim_representative_occurrence.claim_rep_occurrence_id, claim_representative_occurrence.claim_rep_occurrence_ak_id, claim_representative_occurrence.claim_rep_ak_id 
	FROM
	 claim_occurrence, claim_representative_occurrence 
	WHERE
	 claim_representative_occurrence.claim_rep_ak_id=0
	AND claim_occurrence.crrnt_snpsht_flag=1 and claim_occurrence.source_sys_id='PMS' 
	AND claim_occurrence.claim_occurrence_ak_id=claim_representative_occurrence.claim_occurrence_ak_id
),
EXP_SQ_Claim_Representative_Occurrence_PMS_fix AS (
	SELECT
	claim_occurrence_ak_id,
	claim_occurrence_key,
	claim_rep_occurrence_id,
	claim_rep_occurrence_ak_id,
	claim_rep_ak_id,
	-- *INF*: SUBSTR(claim_occurrence_key,1,3)
	SUBSTR(claim_occurrence_key, 1, 3) AS pif_symbol,
	-- *INF*: SUBSTR(claim_occurrence_key,4,7)
	SUBSTR(claim_occurrence_key, 4, 7) AS pif_policy_number,
	-- *INF*: SUBSTR(claim_occurrence_key,11,2)
	SUBSTR(claim_occurrence_key, 11, 2) AS pif_module,
	-- *INF*: TO_CHAR(SUBSTR(claim_occurrence_key,13,2))
	TO_CHAR(SUBSTR(claim_occurrence_key, 13, 2)) AS loss_month,
	-- *INF*: TO_CHAR(SUBSTR(claim_occurrence_key,15,2))
	TO_CHAR(SUBSTR(claim_occurrence_key, 15, 2)) AS loss_day,
	-- *INF*: TO_CHAR(SUBSTR(claim_occurrence_key,17,4))
	TO_CHAR(SUBSTR(claim_occurrence_key, 17, 4)) AS loss_year,
	-- *INF*: TO_CHAR(SUBSTR(claim_occurrence_key,21,3))
	TO_CHAR(SUBSTR(claim_occurrence_key, 21, 3)) AS loss_occurence
	FROM SQ_Claim_Representative_Occurrence_PMS_fix
),
LKP_pif_4578_stage AS (
	SELECT
	loss_adjustor_no,
	loss_examiner,
	pif_symbol,
	pif_policy_number,
	pif_module,
	loss_year,
	loss_month,
	loss_day,
	loss_occurence
	FROM (
		SELECT pif_4578_stage.loss_adjustor_no as loss_adjustor_no, pif_4578_stage.loss_examiner as loss_examiner, pif_4578_stage.pif_symbol as pif_symbol, pif_4578_stage.pif_policy_number as pif_policy_number, pif_4578_stage.pif_module as pif_module, pif_4578_stage.loss_year as loss_year, pif_4578_stage.loss_month as loss_month, pif_4578_stage.loss_day as loss_day, pif_4578_stage.loss_occurence as loss_occurence 
		FROM pif_4578_stage
		WHERE pif_4578_stage.loss_adjustor_no LIKE ('X%') OR pif_4578_stage.loss_adjustor_no IN ('QUE','QUR')
		ORDER BY pif_4578_stage.loss_transaction_date 
		-- TO IGNORE THE DEFAULT ORDER BY
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pif_symbol,pif_policy_number,pif_module,loss_year,loss_month,loss_day,loss_occurence ORDER BY loss_adjustor_no DESC) = 1
),
EXP_Claim_Representative_Occurrence_PMS_fix AS (
	SELECT
	EXP_SQ_Claim_Representative_Occurrence_PMS_fix.claim_rep_occurrence_id,
	LKP_pif_4578_stage.loss_adjustor_no,
	LKP_pif_4578_stage.loss_examiner,
	-- *INF*: ltrim(rtrim(loss_adjustor_no))
	ltrim(rtrim(loss_adjustor_no)) AS V_LOSS_ADJUSTOR_NO,
	-- *INF*: IIF(ISNULL(:LKP.LKP_CLAIM_REP(V_LOSS_ADJUSTOR_NO)),0,:LKP.LKP_CLAIM_REP(V_LOSS_ADJUSTOR_NO))
	IFF(LKP_CLAIM_REP_V_LOSS_ADJUSTOR_NO.claim_rep_ak_id IS NULL, 0, LKP_CLAIM_REP_V_LOSS_ADJUSTOR_NO.claim_rep_ak_id) AS CLAIM_REP_AK_ID_OP
	FROM EXP_SQ_Claim_Representative_Occurrence_PMS_fix
	LEFT JOIN LKP_pif_4578_stage
	ON LKP_pif_4578_stage.pif_symbol = EXP_SQ_Claim_Representative_Occurrence_PMS_fix.pif_symbol AND LKP_pif_4578_stage.pif_policy_number = EXP_SQ_Claim_Representative_Occurrence_PMS_fix.pif_policy_number AND LKP_pif_4578_stage.pif_module = EXP_SQ_Claim_Representative_Occurrence_PMS_fix.pif_module AND LKP_pif_4578_stage.loss_year = EXP_SQ_Claim_Representative_Occurrence_PMS_fix.loss_year AND LKP_pif_4578_stage.loss_month = EXP_SQ_Claim_Representative_Occurrence_PMS_fix.loss_month AND LKP_pif_4578_stage.loss_day = EXP_SQ_Claim_Representative_Occurrence_PMS_fix.loss_day AND LKP_pif_4578_stage.loss_occurence = EXP_SQ_Claim_Representative_Occurrence_PMS_fix.loss_occurence
	LEFT JOIN LKP_CLAIM_REP LKP_CLAIM_REP_V_LOSS_ADJUSTOR_NO
	ON LKP_CLAIM_REP_V_LOSS_ADJUSTOR_NO.claim_rep_key = V_LOSS_ADJUSTOR_NO

),
claim_representative_occurrence1 AS (
	INSERT INTO claim_representative_occurrence
	(claim_rep_occurrence_id, claim_rep_ak_id)
	SELECT 
	CLAIM_REP_OCCURRENCE_ID, 
	CLAIM_REP_AK_ID_OP AS CLAIM_REP_AK_ID
	FROM EXP_Claim_Representative_Occurrence_PMS_fix
),