WITH
SQ_ARCH_PIF_4578_STAGE AS (
	SELECT 
	COUNT(*) as pif_4578_stage_id, 
	pif_symbol + pif_policy_number + pif_module as loss_rec_length, 
	SUM(loss_paid_or_resv_amt) as loss_location_number 
	FROM  dbo.pif_4578_stage_temp 
	WHERE  logical_flag = 0 --No Dummy Transactions
			  AND loss_part = '7'  --No Reinsurrance
	GROUP BY pif_symbol + pif_policy_number + pif_module
	
	
	---- 8/23/2011 Modified the Source Qualifier Query to use pif_4578_stage_temp table instead of arch_pif_4578_stage table.
),
SQ_EDW_TABLES AS (
	SELECT claim_occurrence.pol_key as EDW_pol_key,
			COUNT(*) as EDW_COUNT_of_Transactions,
			SUM(trans_amt) as EDW_SUM_Trans_Amt
	FROM RPT_EDM.dbo.claim_transaction as claim_transaction
			INNER JOIN RPT_EDM.dbo.claimant_coverage_detail as claimant_coverage_detail ON
	claim_transaction.claimant_cov_det_ak_id = claimant_coverage_detail.claimant_cov_det_ak_id
			INNER JOIN RPT_EDM.dbo.claim_party_occurrence as claim_party_occurrence ON
	claimant_coverage_detail.claim_party_occurrence_ak_id = claim_party_occurrence.claim_party_occurrence_ak_id
			INNER JOIN RPT_EDM.dbo.claim_occurrence as claim_occurrence ON
	claim_party_occurrence.claim_occurrence_ak_id = claim_occurrence.claim_occurrence_ak_id
	WHERE claim_transaction.crrnt_snpsht_flag = 1
			AND claimant_coverage_detail.crrnt_snpsht_flag = 1
			AND claim_party_occurrence.crrnt_snpsht_flag = 1
			AND claim_occurrence.crrnt_snpsht_flag = 1
			AND claim_transaction.audit_id <=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	            AND claim_transaction.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	            AND trans_date > '1998-01-01'
	GROUP BY pol_key
),
JNR_EDW_ARCHIVE AS (SELECT
	SQ_EDW_TABLES.EDW_pol_key, 
	SQ_EDW_TABLES.EDW_COUNT_of_Transactions, 
	SQ_EDW_TABLES.EDW_SUM_Trans_Amt, 
	SQ_ARCH_PIF_4578_STAGE.pif_4578_stage_id AS ARCH_COUNT_of_Transactions, 
	SQ_ARCH_PIF_4578_STAGE.loss_rec_length AS ARCH_pol_key, 
	SQ_ARCH_PIF_4578_STAGE.loss_location_number AS ARCH_SUM_Trans_Amt
	FROM SQ_EDW_TABLES
	RIGHT OUTER JOIN SQ_ARCH_PIF_4578_STAGE
	ON SQ_ARCH_PIF_4578_STAGE.loss_rec_length = SQ_EDW_TABLES.EDW_pol_key
),
EXP_Evaluate AS (
	SELECT
	EDW_pol_key,
	EDW_COUNT_of_Transactions,
	EDW_SUM_Trans_Amt,
	ARCH_COUNT_of_Transactions,
	ARCH_pol_key,
	ARCH_SUM_Trans_Amt,
	-- *INF*: IIF(ARCH_SUM_Trans_Amt = EDW_SUM_Trans_Amt,'Y','N')
	IFF(ARCH_SUM_Trans_Amt = EDW_SUM_Trans_Amt,
		'Y',
		'N'
	) AS v_Balance_Amount,
	-- *INF*: IIF(v_Balance_Amount = 'Y', 1, -1)
	IFF(v_Balance_Amount = 'Y',
		1,
		- 1
	) AS err_flag_change,
	err_flag_change AS out_err_flag
	FROM JNR_EDW_ARCHIVE
),
FIL_Err_Flag AS (
	SELECT
	EDW_pol_key, 
	out_err_flag
	FROM EXP_Evaluate
	WHERE TRUE
),
LKP_Pol_id AS (
	SELECT
	policy_key_id,
	pol_key
	FROM (
		SELECT a.pol_id as policy_key_id, 
		                  a.pol_key as pol_key 
		FROM V2.policy a 
		where crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_key ORDER BY policy_key_id) = 1
),
UPD_Policy_Key_Err_Flag AS (
	SELECT
	LKP_Pol_id.policy_key_id, 
	FIL_Err_Flag.out_err_flag
	FROM FIL_Err_Flag
	LEFT JOIN LKP_Pol_id
	ON LKP_Pol_id.pol_key = FIL_Err_Flag.EDW_pol_key
),
policy_update_err_flag_bal_txn AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.policy AS T
	USING UPD_Policy_Key_Err_Flag AS S
	ON T.pol_id = S.policy_key_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.err_flag_bal_txn = S.out_err_flag
),
SQ_ARCH_PIF_4578_STAGE_Bal_Reins_Txn AS (
	SELECT 
	COUNT(*) as pif_4578_stage_id, 
	pif_symbol + pif_policy_number + pif_module as loss_rec_length, 
	SUM(loss_paid_or_resv_amt) as loss_location_number 
	FROM  dbo.pif_4578_stage_temp
	WHERE  	logical_flag in ('0','-1') --No Dummy Transactions
			  AND loss_part = '8'  --Reinsurrance
	GROUP BY pif_symbol + pif_policy_number + pif_module
	
	
	---- 8/23/2011 Modified the Source Qualifier Query to use pif_4578_stage_temp table instead of arch_pif_4578_stage table.
),
SQ_EDW_TABLES_Bal_Reins_Txn AS (
	SELECT claim_occurrence.pol_key as EDW_pol_key,
			COUNT(*) as EDW_COUNT_of_Reins_Transactions,
			SUM(claim_reins_trans_amt) as EDW_SUM_Reins_Trans_Amt
	FROM RPT_EDM.dbo.claim_reinsurance_transaction as claim_reinsurance_transaction
			INNER JOIN RPT_EDM.dbo.claimant_coverage_detail as claimant_coverage_detail ON
	claim_reinsurance_transaction.claimant_cov_det_ak_id = claimant_coverage_detail.claimant_cov_det_ak_id
			INNER JOIN RPT_EDM.dbo.claim_party_occurrence as claim_party_occurrence ON
	claimant_coverage_detail.claim_party_occurrence_ak_id = claim_party_occurrence.claim_party_occurrence_ak_id
			INNER JOIN RPT_EDM.dbo.claim_occurrence as claim_occurrence ON
	claim_party_occurrence.claim_occurrence_ak_id = claim_occurrence.claim_occurrence_ak_id
	WHERE claim_reinsurance_transaction.crrnt_snpsht_flag = 1
			AND claimant_coverage_detail.crrnt_snpsht_flag = 1
			AND claim_party_occurrence.crrnt_snpsht_flag = 1
			AND claim_occurrence.crrnt_snpsht_flag = 1
			AND claim_reinsurance_transaction.audit_id <=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	            AND claim_reinsurance_transaction.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	GROUP BY pol_key
),
JNR_EDW_ARCHIVE_Bal_Reins_Txn AS (SELECT
	SQ_EDW_TABLES_Bal_Reins_Txn.EDW_pol_key, 
	SQ_EDW_TABLES_Bal_Reins_Txn.EDW_COUNT_of_Transactions, 
	SQ_EDW_TABLES_Bal_Reins_Txn.EDW_SUM_Trans_Amt, 
	SQ_ARCH_PIF_4578_STAGE_Bal_Reins_Txn.pif_4578_stage_id AS ARCH_COUNT_of_Transactions, 
	SQ_ARCH_PIF_4578_STAGE_Bal_Reins_Txn.loss_rec_length AS ARCH_pol_key, 
	SQ_ARCH_PIF_4578_STAGE_Bal_Reins_Txn.loss_location_number AS ARCH_SUM_Trans_Amt
	FROM SQ_EDW_TABLES_Bal_Reins_Txn
	RIGHT OUTER JOIN SQ_ARCH_PIF_4578_STAGE_Bal_Reins_Txn
	ON SQ_ARCH_PIF_4578_STAGE_Bal_Reins_Txn.loss_rec_length = SQ_EDW_TABLES_Bal_Reins_Txn.EDW_pol_key
),
EXP_Evaluate_Bal_Reins_Txn AS (
	SELECT
	EDW_pol_key,
	EDW_COUNT_of_Transactions,
	EDW_SUM_Trans_Amt,
	ARCH_COUNT_of_Transactions,
	ARCH_pol_key,
	ARCH_SUM_Trans_Amt,
	-- *INF*: IIF(ARCH_SUM_Trans_Amt = EDW_SUM_Trans_Amt,'Y','N')
	IFF(ARCH_SUM_Trans_Amt = EDW_SUM_Trans_Amt,
		'Y',
		'N'
	) AS v_Balance_Amount,
	-- *INF*: IIF(v_Balance_Amount = 'Y', 1, -1)
	IFF(v_Balance_Amount = 'Y',
		1,
		- 1
	) AS err_flag_change,
	err_flag_change AS out_err_flag_reins_txn
	FROM JNR_EDW_ARCHIVE_Bal_Reins_Txn
),
FIL_Err_Flag_Bal_Reins_Txn AS (
	SELECT
	EDW_pol_key, 
	out_err_flag_reins_txn
	FROM EXP_Evaluate_Bal_Reins_Txn
	WHERE TRUE
),
LKP_Pol_id_bal_reins_txn AS (
	SELECT
	policy_key_id,
	pol_key
	FROM (
		SELECT a.pol_id as policy_key_id, 
		                  a.pol_key as pol_key 
		 FROM V2.policy a 
		where crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_key ORDER BY policy_key_id) = 1
),
UPD_Policy_Key_Err_Flag_bal_reins AS (
	SELECT
	LKP_Pol_id_bal_reins_txn.policy_key_id, 
	FIL_Err_Flag_Bal_Reins_Txn.out_err_flag_reins_txn
	FROM FIL_Err_Flag_Bal_Reins_Txn
	LEFT JOIN LKP_Pol_id_bal_reins_txn
	ON LKP_Pol_id_bal_reins_txn.pol_key = FIL_Err_Flag_Bal_Reins_Txn.EDW_pol_key
),
policy_update_err_flag_bal_reins_txn AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.policy AS T
	USING UPD_Policy_Key_Err_Flag_bal_reins AS S
	ON T.pol_id = S.policy_key_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.err_flag_bal_reins = S.out_err_flag_reins_txn
),