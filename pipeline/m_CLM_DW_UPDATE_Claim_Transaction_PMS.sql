WITH
LKP_claim_transaction AS (
	SELECT
	trans_date,
	claimant_cov_det_ak_id,
	cause_of_loss,
	reserve_ctgry,
	offset_onset_ind,
	financial_type_code,
	trans_code,
	trans_ctgry_code
	FROM (
		SELECT claim_transaction.claimant_cov_det_ak_id as claimant_cov_det_ak_id, claim_transaction.cause_of_loss as cause_of_loss, claim_transaction.reserve_ctgry as reserve_ctgry, claim_transaction.offset_onset_ind as offset_onset_ind, claim_transaction.financial_type_code as financial_type_code, claim_transaction.trans_code as trans_code, claim_transaction.trans_ctgry_code as trans_ctgry_code, claim_transaction.trans_date as trans_date FROM claim_transaction
		WHERE SOURCE_SYS_ID='PMS'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claimant_cov_det_ak_id,cause_of_loss,reserve_ctgry,offset_onset_ind,financial_type_code,trans_code,trans_ctgry_code,trans_date ORDER BY trans_date DESC) = 1
),
SQ_claim_transaction AS (
	SELECT claim_transaction.claim_trans_id, claim_transaction.claim_trans_ak_id, claim_transaction.claimant_cov_det_ak_id, claim_transaction.cause_of_loss, claim_transaction.reserve_ctgry, claim_transaction.offset_onset_ind, claim_transaction.financial_type_code, claim_transaction.pms_trans_code, claim_transaction.trans_code, claim_transaction.trans_date, claim_transaction.trans_ctgry_code, claim_transaction.trans_amt, claim_transaction.source_sys_id 
	FROM
	 claim_transaction 
	WHERE
	 claim_transaction.source_sys_id='PMS' AND claim_transaction.trans_code='0'
	and claim_transaction.pms_trans_code in ('95','97','98','99')
),
EXP_UPDATE_Claim_Transaction_PMS AS (
	SELECT
	claim_trans_id,
	claim_trans_ak_id,
	claimant_cov_det_ak_id,
	cause_of_loss,
	reserve_ctgry,
	offset_onset_ind,
	financial_type_code,
	pms_trans_code,
	trans_code,
	trans_date,
	trans_ctgry_code,
	trans_amt,
	source_sys_id,
	-- *INF*: :LKP.LKP_CLAIM_TRANSACTION(claimant_cov_det_ak_id,cause_of_loss,reserve_ctgry,offset_onset_ind,financial_type_code,'40',trans_ctgry_code, trans_date)
	LKP_CLAIM_TRANSACTION_claimant_cov_det_ak_id_cause_of_loss_reserve_ctgry_offset_onset_ind_financial_type_code_40_trans_ctgry_code_trans_date.trans_date AS Trans_date_40,
	-- *INF*: :LKP.LKP_CLAIM_TRANSACTION(claimant_cov_det_ak_id,cause_of_loss,reserve_ctgry,offset_onset_ind,financial_type_code,'90',trans_ctgry_code, trans_date)
	LKP_CLAIM_TRANSACTION_claimant_cov_det_ak_id_cause_of_loss_reserve_ctgry_offset_onset_ind_financial_type_code_90_trans_ctgry_code_trans_date.trans_date AS Trans_date_90,
	-- *INF*: :LKP.LKP_CLAIM_TRANSACTION(claimant_cov_det_ak_id,cause_of_loss,reserve_ctgry,offset_onset_ind,financial_type_code,'92',trans_ctgry_code, trans_date)
	LKP_CLAIM_TRANSACTION_claimant_cov_det_ak_id_cause_of_loss_reserve_ctgry_offset_onset_ind_financial_type_code_92_trans_ctgry_code_trans_date.trans_date AS Trans_date_92,
	-- *INF*: IIF(Trans_date_90>Trans_date_92,Trans_date_90,Trans_date_92)
	IFF(Trans_date_90 > Trans_date_92,
		Trans_date_90,
		Trans_date_92
	) AS greater_trans_date_90_92,
	-- *INF*: IIF(
	--     ISNULL(Trans_date_40) AND ISNULL(Trans_date_90) AND ISNULL(Trans_date_92),
	--     '90', 
	--     (IIF(
	-- 	NOT ISNULL(Trans_date_40) AND ISNULL(Trans_date_90) AND ISNULL(Trans_date_92), 
	-- 	'92',
	-- 	(IIF(
	-- 	    (NOT ISNULL(Trans_date_90) OR NOT ISNULL(Trans_date_92)) AND NOT ISNULL(Trans_date_40), 
	-- 	    IIF(
	-- 		Trans_date_40>=greater_trans_date_90_92,
	-- 		'92',
	-- 		'65'),
	-- 	    '65')))))
	IFF(Trans_date_40 IS NULL 
		AND Trans_date_90 IS NULL 
		AND Trans_date_92 IS NULL,
		'90',
		( IFF(Trans_date_40 IS NULL 
				AND Trans_date_90 IS NULL 
				AND Trans_date_92 IS NOT NULL,
				'92',
				( IFF(( Trans_date_90 IS NULL 
							OR Trans_date_92 IS NOT NOT NULL 
						) 
						AND Trans_date_40 IS NOT NULL,
						IFF(Trans_date_40 >= greater_trans_date_90_92,
							'92',
							'65'
						),
						'65'
					) 
				)
			) 
		)
	) AS trans_code_op
	FROM SQ_claim_transaction
	LEFT JOIN LKP_CLAIM_TRANSACTION LKP_CLAIM_TRANSACTION_claimant_cov_det_ak_id_cause_of_loss_reserve_ctgry_offset_onset_ind_financial_type_code_40_trans_ctgry_code_trans_date
	ON LKP_CLAIM_TRANSACTION_claimant_cov_det_ak_id_cause_of_loss_reserve_ctgry_offset_onset_ind_financial_type_code_40_trans_ctgry_code_trans_date.claimant_cov_det_ak_id = claimant_cov_det_ak_id
	AND LKP_CLAIM_TRANSACTION_claimant_cov_det_ak_id_cause_of_loss_reserve_ctgry_offset_onset_ind_financial_type_code_40_trans_ctgry_code_trans_date.cause_of_loss = cause_of_loss
	AND LKP_CLAIM_TRANSACTION_claimant_cov_det_ak_id_cause_of_loss_reserve_ctgry_offset_onset_ind_financial_type_code_40_trans_ctgry_code_trans_date.reserve_ctgry = reserve_ctgry
	AND LKP_CLAIM_TRANSACTION_claimant_cov_det_ak_id_cause_of_loss_reserve_ctgry_offset_onset_ind_financial_type_code_40_trans_ctgry_code_trans_date.offset_onset_ind = offset_onset_ind
	AND LKP_CLAIM_TRANSACTION_claimant_cov_det_ak_id_cause_of_loss_reserve_ctgry_offset_onset_ind_financial_type_code_40_trans_ctgry_code_trans_date.financial_type_code = financial_type_code
	AND LKP_CLAIM_TRANSACTION_claimant_cov_det_ak_id_cause_of_loss_reserve_ctgry_offset_onset_ind_financial_type_code_40_trans_ctgry_code_trans_date.trans_code = '40'
	AND LKP_CLAIM_TRANSACTION_claimant_cov_det_ak_id_cause_of_loss_reserve_ctgry_offset_onset_ind_financial_type_code_40_trans_ctgry_code_trans_date.trans_ctgry_code = trans_ctgry_code
	AND LKP_CLAIM_TRANSACTION_claimant_cov_det_ak_id_cause_of_loss_reserve_ctgry_offset_onset_ind_financial_type_code_40_trans_ctgry_code_trans_date.trans_date = trans_date

	LEFT JOIN LKP_CLAIM_TRANSACTION LKP_CLAIM_TRANSACTION_claimant_cov_det_ak_id_cause_of_loss_reserve_ctgry_offset_onset_ind_financial_type_code_90_trans_ctgry_code_trans_date
	ON LKP_CLAIM_TRANSACTION_claimant_cov_det_ak_id_cause_of_loss_reserve_ctgry_offset_onset_ind_financial_type_code_90_trans_ctgry_code_trans_date.claimant_cov_det_ak_id = claimant_cov_det_ak_id
	AND LKP_CLAIM_TRANSACTION_claimant_cov_det_ak_id_cause_of_loss_reserve_ctgry_offset_onset_ind_financial_type_code_90_trans_ctgry_code_trans_date.cause_of_loss = cause_of_loss
	AND LKP_CLAIM_TRANSACTION_claimant_cov_det_ak_id_cause_of_loss_reserve_ctgry_offset_onset_ind_financial_type_code_90_trans_ctgry_code_trans_date.reserve_ctgry = reserve_ctgry
	AND LKP_CLAIM_TRANSACTION_claimant_cov_det_ak_id_cause_of_loss_reserve_ctgry_offset_onset_ind_financial_type_code_90_trans_ctgry_code_trans_date.offset_onset_ind = offset_onset_ind
	AND LKP_CLAIM_TRANSACTION_claimant_cov_det_ak_id_cause_of_loss_reserve_ctgry_offset_onset_ind_financial_type_code_90_trans_ctgry_code_trans_date.financial_type_code = financial_type_code
	AND LKP_CLAIM_TRANSACTION_claimant_cov_det_ak_id_cause_of_loss_reserve_ctgry_offset_onset_ind_financial_type_code_90_trans_ctgry_code_trans_date.trans_code = '90'
	AND LKP_CLAIM_TRANSACTION_claimant_cov_det_ak_id_cause_of_loss_reserve_ctgry_offset_onset_ind_financial_type_code_90_trans_ctgry_code_trans_date.trans_ctgry_code = trans_ctgry_code
	AND LKP_CLAIM_TRANSACTION_claimant_cov_det_ak_id_cause_of_loss_reserve_ctgry_offset_onset_ind_financial_type_code_90_trans_ctgry_code_trans_date.trans_date = trans_date

	LEFT JOIN LKP_CLAIM_TRANSACTION LKP_CLAIM_TRANSACTION_claimant_cov_det_ak_id_cause_of_loss_reserve_ctgry_offset_onset_ind_financial_type_code_92_trans_ctgry_code_trans_date
	ON LKP_CLAIM_TRANSACTION_claimant_cov_det_ak_id_cause_of_loss_reserve_ctgry_offset_onset_ind_financial_type_code_92_trans_ctgry_code_trans_date.claimant_cov_det_ak_id = claimant_cov_det_ak_id
	AND LKP_CLAIM_TRANSACTION_claimant_cov_det_ak_id_cause_of_loss_reserve_ctgry_offset_onset_ind_financial_type_code_92_trans_ctgry_code_trans_date.cause_of_loss = cause_of_loss
	AND LKP_CLAIM_TRANSACTION_claimant_cov_det_ak_id_cause_of_loss_reserve_ctgry_offset_onset_ind_financial_type_code_92_trans_ctgry_code_trans_date.reserve_ctgry = reserve_ctgry
	AND LKP_CLAIM_TRANSACTION_claimant_cov_det_ak_id_cause_of_loss_reserve_ctgry_offset_onset_ind_financial_type_code_92_trans_ctgry_code_trans_date.offset_onset_ind = offset_onset_ind
	AND LKP_CLAIM_TRANSACTION_claimant_cov_det_ak_id_cause_of_loss_reserve_ctgry_offset_onset_ind_financial_type_code_92_trans_ctgry_code_trans_date.financial_type_code = financial_type_code
	AND LKP_CLAIM_TRANSACTION_claimant_cov_det_ak_id_cause_of_loss_reserve_ctgry_offset_onset_ind_financial_type_code_92_trans_ctgry_code_trans_date.trans_code = '92'
	AND LKP_CLAIM_TRANSACTION_claimant_cov_det_ak_id_cause_of_loss_reserve_ctgry_offset_onset_ind_financial_type_code_92_trans_ctgry_code_trans_date.trans_ctgry_code = trans_ctgry_code
	AND LKP_CLAIM_TRANSACTION_claimant_cov_det_ak_id_cause_of_loss_reserve_ctgry_offset_onset_ind_financial_type_code_92_trans_ctgry_code_trans_date.trans_date = trans_date

),
EXP_UPD_Claim_Transaction_PMS AS (
	SELECT
	claim_trans_id,
	trans_code_op,
	sysdate AS modified_date_op
	FROM EXP_UPDATE_Claim_Transaction_PMS
),
claim_transaction1 AS (
	INSERT INTO claim_transaction
	(claim_trans_id, trans_code, modified_date)
	SELECT 
	CLAIM_TRANS_ID, 
	trans_code_op AS TRANS_CODE, 
	modified_date_op AS MODIFIED_DATE
	FROM EXP_UPD_Claim_Transaction_PMS
),