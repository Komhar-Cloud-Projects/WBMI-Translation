WITH
LKP_Claim_Reinsurance_Trasaction AS (
	SELECT
	claim_reins_trans_date,
	claimant_cov_det_ak_id,
	reins_cov_ak_id,
	claim_reins_financial_type_code,
	claim_reins_trans_code,
	offset_onset_ind
	FROM (
		SELECT CRT.claimant_cov_det_ak_id          AS claimant_cov_det_ak_id,
		       CRT.reins_cov_ak_id                 AS reins_cov_ak_id,
		       CRT.claim_reins_financial_type_code AS claim_reins_financial_type_code,
		       CRT.claim_reins_trans_code          AS claim_reins_trans_code,
		       CRT.claim_reins_trans_date          AS claim_reins_trans_date,
		       CRT.offset_onset_ind                AS offset_onset_ind
		FROM   claim_reinsurance_transaction  CRT
		ORDER BY claim_reins_trans_date  -- // Commenting order by clause
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claimant_cov_det_ak_id,reins_cov_ak_id,claim_reins_financial_type_code,claim_reins_trans_code,claim_reins_trans_date,offset_onset_ind ORDER BY claim_reins_trans_date) = 1
),
SQ_claim_reinsurance_transaction AS (
	SELECT CRT.claim_reins_trans_id,
	       CRT.claim_reins_trans_ak_id,
	       CRT.claimant_cov_det_ak_id,
	       CRT.reins_cov_ak_id,
	       CRT.claim_reins_trans_base_type_code,
	       CRT.claim_reins_financial_type_code,
	       CRT.claim_reins_trans_code,
	       CRT.claim_reins_trans_amt,
	       CRT.claim_reins_trans_hist_amt,
	       CRT.claim_reins_trans_date,
	       CRT.claim_reins_acct_entered_date,
	       CRT.offset_onset_ind,
	       CRT.claim_reins_pms_trans_code
	FROM   @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_reinsurance_transaction CRT
	WHERE  CRT.claim_reins_trans_code = '0'
	       AND CRT.claim_reins_pms_trans_code IN ( '95', '97', '98', '99' )
),
EXP_Get_Transcode AS (
	SELECT
	claim_reins_trans_id,
	claim_reins_trans_ak_id,
	claimant_cov_det_ak_id,
	reins_cov_ak_id,
	claim_reins_trans_base_type_code,
	claim_reins_financial_type_code,
	claim_reins_trans_code,
	claim_reins_trans_amt,
	claim_reins_trans_hist_amt,
	claim_reins_trans_date,
	offset_onset_ind,
	claim_reins_acct_entered_date,
	claim_reins_pms_trans_code,
	-- *INF*: :LKP.LKP_CLAIM_REINSURANCE_TRASACTION(claimant_cov_det_ak_id, reins_cov_ak_id,claim_reins_financial_type_code, '40',claim_reins_trans_date, offset_onset_ind)
	LKP_CLAIM_REINSURANCE_TRASACTION_claimant_cov_det_ak_id_reins_cov_ak_id_claim_reins_financial_type_code_40_claim_reins_trans_date_offset_onset_ind.claim_reins_trans_date AS V_Trans_Date_40,
	-- *INF*: :LKP.LKP_CLAIM_REINSURANCE_TRASACTION(claimant_cov_det_ak_id, reins_cov_ak_id, claim_reins_financial_type_code, '90',claim_reins_trans_date, offset_onset_ind)
	LKP_CLAIM_REINSURANCE_TRASACTION_claimant_cov_det_ak_id_reins_cov_ak_id_claim_reins_financial_type_code_90_claim_reins_trans_date_offset_onset_ind.claim_reins_trans_date AS V_Trans_Date_90,
	-- *INF*: :LKP.LKP_CLAIM_REINSURANCE_TRASACTION(claimant_cov_det_ak_id, reins_cov_ak_id, claim_reins_financial_type_code, '92',claim_reins_trans_date, offset_onset_ind)
	LKP_CLAIM_REINSURANCE_TRASACTION_claimant_cov_det_ak_id_reins_cov_ak_id_claim_reins_financial_type_code_92_claim_reins_trans_date_offset_onset_ind.claim_reins_trans_date AS V_Trans_Date_92,
	-- *INF*: IIF(V_Trans_Date_90>V_Trans_Date_92,V_Trans_Date_90,V_Trans_Date_92)
	IFF(V_Trans_Date_90 > V_Trans_Date_92,
		V_Trans_Date_90,
		V_Trans_Date_92
	) AS V_greater_trans_date_90_92,
	-- *INF*: IIF(
	--     ISNULL(V_Trans_Date_40) AND ISNULL(V_Trans_Date_90) AND ISNULL(V_Trans_Date_92),
	--     '90', 
	--     (IIF(
	-- 	NOT ISNULL(V_Trans_Date_40) AND ISNULL(V_Trans_Date_90) AND ISNULL(V_Trans_Date_92), 
	-- 	'92',
	-- 	(IIF(
	-- 	    (NOT ISNULL(V_Trans_Date_90) OR NOT ISNULL(V_Trans_Date_92)) AND NOT ISNULL(V_Trans_Date_40), 
	-- 	    IIF(
	-- 		V_Trans_Date_40>=V_greater_trans_date_90_92,
	-- 		'92',
	-- 		'65'),
	-- 	    '65')))))
	IFF(V_Trans_Date_40 IS NULL 
		AND V_Trans_Date_90 IS NULL 
		AND V_Trans_Date_92 IS NULL,
		'90',
		( IFF(V_Trans_Date_40 IS NULL 
				AND V_Trans_Date_90 IS NULL 
				AND V_Trans_Date_92 IS NOT NULL,
				'92',
				( IFF(( V_Trans_Date_90 IS NULL 
							OR V_Trans_Date_92 IS NOT NOT NULL 
						) 
						AND V_Trans_Date_40 IS NOT NULL,
						IFF(V_Trans_Date_40 >= V_greater_trans_date_90_92,
							'92',
							'65'
						),
						'65'
					) 
				)
			) 
		)
	) AS OUT_Claim_Reins_Trans_Code
	FROM SQ_claim_reinsurance_transaction
	LEFT JOIN LKP_CLAIM_REINSURANCE_TRASACTION LKP_CLAIM_REINSURANCE_TRASACTION_claimant_cov_det_ak_id_reins_cov_ak_id_claim_reins_financial_type_code_40_claim_reins_trans_date_offset_onset_ind
	ON LKP_CLAIM_REINSURANCE_TRASACTION_claimant_cov_det_ak_id_reins_cov_ak_id_claim_reins_financial_type_code_40_claim_reins_trans_date_offset_onset_ind.claimant_cov_det_ak_id = claimant_cov_det_ak_id
	AND LKP_CLAIM_REINSURANCE_TRASACTION_claimant_cov_det_ak_id_reins_cov_ak_id_claim_reins_financial_type_code_40_claim_reins_trans_date_offset_onset_ind.reins_cov_ak_id = reins_cov_ak_id
	AND LKP_CLAIM_REINSURANCE_TRASACTION_claimant_cov_det_ak_id_reins_cov_ak_id_claim_reins_financial_type_code_40_claim_reins_trans_date_offset_onset_ind.claim_reins_financial_type_code = claim_reins_financial_type_code
	AND LKP_CLAIM_REINSURANCE_TRASACTION_claimant_cov_det_ak_id_reins_cov_ak_id_claim_reins_financial_type_code_40_claim_reins_trans_date_offset_onset_ind.claim_reins_trans_code = '40'
	AND LKP_CLAIM_REINSURANCE_TRASACTION_claimant_cov_det_ak_id_reins_cov_ak_id_claim_reins_financial_type_code_40_claim_reins_trans_date_offset_onset_ind.claim_reins_trans_date = claim_reins_trans_date
	AND LKP_CLAIM_REINSURANCE_TRASACTION_claimant_cov_det_ak_id_reins_cov_ak_id_claim_reins_financial_type_code_40_claim_reins_trans_date_offset_onset_ind.offset_onset_ind = offset_onset_ind

	LEFT JOIN LKP_CLAIM_REINSURANCE_TRASACTION LKP_CLAIM_REINSURANCE_TRASACTION_claimant_cov_det_ak_id_reins_cov_ak_id_claim_reins_financial_type_code_90_claim_reins_trans_date_offset_onset_ind
	ON LKP_CLAIM_REINSURANCE_TRASACTION_claimant_cov_det_ak_id_reins_cov_ak_id_claim_reins_financial_type_code_90_claim_reins_trans_date_offset_onset_ind.claimant_cov_det_ak_id = claimant_cov_det_ak_id
	AND LKP_CLAIM_REINSURANCE_TRASACTION_claimant_cov_det_ak_id_reins_cov_ak_id_claim_reins_financial_type_code_90_claim_reins_trans_date_offset_onset_ind.reins_cov_ak_id = reins_cov_ak_id
	AND LKP_CLAIM_REINSURANCE_TRASACTION_claimant_cov_det_ak_id_reins_cov_ak_id_claim_reins_financial_type_code_90_claim_reins_trans_date_offset_onset_ind.claim_reins_financial_type_code = claim_reins_financial_type_code
	AND LKP_CLAIM_REINSURANCE_TRASACTION_claimant_cov_det_ak_id_reins_cov_ak_id_claim_reins_financial_type_code_90_claim_reins_trans_date_offset_onset_ind.claim_reins_trans_code = '90'
	AND LKP_CLAIM_REINSURANCE_TRASACTION_claimant_cov_det_ak_id_reins_cov_ak_id_claim_reins_financial_type_code_90_claim_reins_trans_date_offset_onset_ind.claim_reins_trans_date = claim_reins_trans_date
	AND LKP_CLAIM_REINSURANCE_TRASACTION_claimant_cov_det_ak_id_reins_cov_ak_id_claim_reins_financial_type_code_90_claim_reins_trans_date_offset_onset_ind.offset_onset_ind = offset_onset_ind

	LEFT JOIN LKP_CLAIM_REINSURANCE_TRASACTION LKP_CLAIM_REINSURANCE_TRASACTION_claimant_cov_det_ak_id_reins_cov_ak_id_claim_reins_financial_type_code_92_claim_reins_trans_date_offset_onset_ind
	ON LKP_CLAIM_REINSURANCE_TRASACTION_claimant_cov_det_ak_id_reins_cov_ak_id_claim_reins_financial_type_code_92_claim_reins_trans_date_offset_onset_ind.claimant_cov_det_ak_id = claimant_cov_det_ak_id
	AND LKP_CLAIM_REINSURANCE_TRASACTION_claimant_cov_det_ak_id_reins_cov_ak_id_claim_reins_financial_type_code_92_claim_reins_trans_date_offset_onset_ind.reins_cov_ak_id = reins_cov_ak_id
	AND LKP_CLAIM_REINSURANCE_TRASACTION_claimant_cov_det_ak_id_reins_cov_ak_id_claim_reins_financial_type_code_92_claim_reins_trans_date_offset_onset_ind.claim_reins_financial_type_code = claim_reins_financial_type_code
	AND LKP_CLAIM_REINSURANCE_TRASACTION_claimant_cov_det_ak_id_reins_cov_ak_id_claim_reins_financial_type_code_92_claim_reins_trans_date_offset_onset_ind.claim_reins_trans_code = '92'
	AND LKP_CLAIM_REINSURANCE_TRASACTION_claimant_cov_det_ak_id_reins_cov_ak_id_claim_reins_financial_type_code_92_claim_reins_trans_date_offset_onset_ind.claim_reins_trans_date = claim_reins_trans_date
	AND LKP_CLAIM_REINSURANCE_TRASACTION_claimant_cov_det_ak_id_reins_cov_ak_id_claim_reins_financial_type_code_92_claim_reins_trans_date_offset_onset_ind.offset_onset_ind = offset_onset_ind

),
EXP_Set_Modified_Date AS (
	SELECT
	claim_reins_trans_id,
	OUT_Claim_Reins_Trans_Code AS Claim_Reins_Trans_Code,
	SYSDATE AS Modified_Date
	FROM EXP_Get_Transcode
),
claim_reinsurance_transaction1 AS (
	INSERT INTO claim_reinsurance_transaction
	(claim_reins_trans_id, modified_date, claim_reins_trans_code)
	SELECT 
	CLAIM_REINS_TRANS_ID, 
	Modified_Date AS MODIFIED_DATE, 
	Claim_Reins_Trans_Code AS CLAIM_REINS_TRANS_CODE
	FROM EXP_Set_Modified_Date
),