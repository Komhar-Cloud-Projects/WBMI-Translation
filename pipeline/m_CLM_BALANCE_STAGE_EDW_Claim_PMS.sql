WITH
SQ_ARCH_PIF_4578_STAGE AS (
	SELECT COUNT(*) as pif_4578_stage_id,
				(pif_symbol + pif_policy_number + pif_module +
	                	(CASE LEN(CONVERT(varchar(2), loss_month)) WHEN 1 THEN '0' + CONVERT(varchar(2), loss_month) 
	                     	ELSE CONVERT(varchar(2), loss_month) END) +
				(CASE LEN(CONVERT(varchar(2), loss_day)) WHEN 1 THEN '0' + CONVERT(varchar(2), loss_day) 
	                     	ELSE CONVERT(varchar(2), loss_day) END) + CONVERT(varchar(4), loss_year)+
				loss_occurence) as loss_rec_length, 
				SUM(loss_paid_or_resv_amt) as loss_location_number 
	FROM @{pipeline().parameters.DB_NAME_STAGE}.dbo.pif_4578_stage_temp
				WHERE logical_flag = 0 --No Dummy Transactions
			  	AND loss_part = '7'  --No Reinsurrance
	GROUP BY 	(pif_symbol + pif_policy_number + pif_module + 
				(CASE LEN(CONVERT(varchar(2), loss_month)) WHEN 1 THEN '0' + CONVERT(varchar(2), loss_month) 
	                     	ELSE CONVERT(varchar(2), loss_month) END)+
				(CASE LEN(CONVERT(varchar(2), loss_day)) WHEN 1 THEN '0' + CONVERT(varchar(2), loss_day) 
	                     	ELSE CONVERT(varchar(2), loss_day) END) + CONVERT(varchar(4), loss_year)+
				loss_occurence)
	
	
	---- 8/23/2011 Modified the Source Qualifier Query to use pif_4578_stage_temp table instead of arch_pif_4578_stage table.
),
SQ_EDW_TABLES_Balance_Transaction AS (
	SELECT claim_occurrence.claim_occurrence_key as EDW_CO_key,
			COUNT(*) as EDW_COUNT_of_Transactions,
			SUM(trans_amt) as EDW_SUM_Trans_Amt
	FROM @{pipeline().parameters.DB_NAME_EDW}.dbo.claim_transaction as claim_transaction
			INNER JOIN @{pipeline().parameters.DB_NAME_EDW}.dbo.claimant_coverage_detail as claimant_coverage_detail ON
	claim_transaction.claimant_cov_det_ak_id = claimant_coverage_detail.claimant_cov_det_ak_id
			INNER JOIN @{pipeline().parameters.DB_NAME_EDW}.dbo.claim_party_occurrence as claim_party_occurrence ON
	claimant_coverage_detail.claim_party_occurrence_ak_id = claim_party_occurrence.claim_party_occurrence_ak_id
			INNER JOIN @{pipeline().parameters.DB_NAME_EDW}.dbo.claim_occurrence as claim_occurrence ON
	claim_party_occurrence.claim_occurrence_ak_id = claim_occurrence.claim_occurrence_ak_id
			WHERE claim_transaction.crrnt_snpsht_flag = 1
			AND claimant_coverage_detail.crrnt_snpsht_flag = 1
			AND claim_party_occurrence.crrnt_snpsht_flag = 1
			AND claim_occurrence.crrnt_snpsht_flag = 1
			AND claim_transaction.audit_id <= @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} 
	            AND claim_transaction.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	            AND trans_date > '1998-01-01'
	GROUP BY claim_occurrence_key
),
JNR_EDW_ARCHIVE AS (SELECT
	SQ_EDW_TABLES_Balance_Transaction.EDW_claim_occurrence_key, 
	SQ_EDW_TABLES_Balance_Transaction.EDW_COUNT_of_Transactions, 
	SQ_EDW_TABLES_Balance_Transaction.EDW_SUM_Trans_Amt, 
	SQ_ARCH_PIF_4578_STAGE.pif_4578_stage_id AS ARCH_COUNT_of_Transactions, 
	SQ_ARCH_PIF_4578_STAGE.loss_rec_length AS ARCH_claim_occurrence_key, 
	SQ_ARCH_PIF_4578_STAGE.loss_location_number AS ARCH_SUM_Trans_Amt
	FROM SQ_EDW_TABLES_Balance_Transaction
	RIGHT OUTER JOIN SQ_ARCH_PIF_4578_STAGE
	ON SQ_ARCH_PIF_4578_STAGE.loss_rec_length = SQ_EDW_TABLES_Balance_Transaction.EDW_claim_occurrence_key
),
EXP_Evaluate AS (
	SELECT
	EDW_claim_occurrence_key,
	EDW_COUNT_of_Transactions,
	EDW_SUM_Trans_Amt,
	ARCH_COUNT_of_Transactions,
	ARCH_claim_occurrence_key,
	ARCH_SUM_Trans_Amt,
	-- *INF*: IIF(ARCH_SUM_Trans_Amt = EDW_SUM_Trans_Amt,'Y','N')
	IFF(ARCH_SUM_Trans_Amt = EDW_SUM_Trans_Amt, 'Y', 'N') AS v_Balance_Amount,
	-- *INF*: DECODE(TRUE,
	-- EDW_SUM_Trans_Amt <> ARCH_SUM_Trans_Amt , -1,
	-- EDW_SUM_Trans_Amt = ARCH_SUM_Trans_Amt 
	--    AND EDW_COUNT_of_Transactions != ARCH_COUNT_of_Transactions, -2,
	-- 0)
	-- 
	-- 
	--  ---- (-1,-2) is used to identify the PMS claims that are not balancing either by amount or no. of transactions.
	--  ---- (-3,-4) is used to identify EXCEED claims that are not balancing either by amount or no. of transactions.
	-- 
	-- 
	-- ---  IIF(v_Balance_Amount = 'Y', 1, -1)
	-- 
	-- 
	DECODE(TRUE,
	EDW_SUM_Trans_Amt <> ARCH_SUM_Trans_Amt, - 1,
	EDW_SUM_Trans_Amt = ARCH_SUM_Trans_Amt AND EDW_COUNT_of_Transactions != ARCH_COUNT_of_Transactions, - 2,
	0) AS err_flag_change,
	err_flag_change AS out_err_flag_bal_txn
	FROM JNR_EDW_ARCHIVE
),
FIL_Err_Flag AS (
	SELECT
	EDW_claim_occurrence_key, 
	out_err_flag_bal_txn
	FROM EXP_Evaluate
	WHERE TRUE
),
LKP_Claim_Occurrence_id AS (
	SELECT
	claim_occurrence_id,
	claim_occurrence_key
	FROM (
		SELECT 
		a.claim_occurrence_id as claim_occurrence_id, 
		a.claim_occurrence_key as claim_occurrence_key 
		FROM claim_occurrence a
		WHERE source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' and crrnt_snpsht_flag =1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_occurrence_key ORDER BY claim_occurrence_id) = 1
),
UPD_Claim_Occurrence_Err_Flag_bal_txn AS (
	SELECT
	LKP_Claim_Occurrence_id.claim_occurrence_id, 
	FIL_Err_Flag.out_err_flag_bal_txn
	FROM FIL_Err_Flag
	LEFT JOIN LKP_Claim_Occurrence_id
	ON LKP_Claim_Occurrence_id.claim_occurrence_key = FIL_Err_Flag.EDW_claim_occurrence_key
),
claim_occurrence_update_err_flag_bal_txn AS (
	MERGE INTO claim_occurrence AS T
	USING UPD_Claim_Occurrence_Err_Flag_bal_txn AS S
	ON T.claim_occurrence_id = S.claim_occurrence_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.err_flag_bal_txn = S.out_err_flag_bal_txn
),
SQ_ARCH_PIF_4578_STAGE_Bal_Reins_Txn AS (
	SELECT COUNT(*) as pif_4578_stage_id,
				(pif_symbol + pif_policy_number + pif_module +
	                	(CASE LEN(CONVERT(varchar(2), loss_month)) WHEN 1 THEN '0' + CONVERT(varchar(2), loss_month) 
	                     	ELSE CONVERT(varchar(2), loss_month) END) +
				(CASE LEN(CONVERT(varchar(2), loss_day)) WHEN 1 THEN '0' + CONVERT(varchar(2), loss_day) 
	                     	ELSE CONVERT(varchar(2), loss_day) END) + CONVERT(varchar(4), loss_year)+
				loss_occurence) as loss_rec_length, 
				SUM(loss_paid_or_resv_amt) as loss_location_number 
	FROM @{pipeline().parameters.DB_NAME_STAGE}.dbo.pif_4578_stage_temp
				WHERE logical_flag in ('0','-1') --No Dummy Transactions
			  	 AND loss_part = '8' --Reinsurrance
	GROUP BY 	(pif_symbol + pif_policy_number + pif_module + 
				(CASE LEN(CONVERT(varchar(2), loss_month)) WHEN 1 THEN '0' + CONVERT(varchar(2), loss_month) 
	                     	ELSE CONVERT(varchar(2), loss_month) END)+
				(CASE LEN(CONVERT(varchar(2), loss_day)) WHEN 1 THEN '0' + CONVERT(varchar(2), loss_day) 
	                     	ELSE CONVERT(varchar(2), loss_day) END) + CONVERT(varchar(4), loss_year)+
				loss_occurence)
	
	
	---- 8/23/2011 Modified the Source Qualifier Query to use pif_4578_stage_temp table instead of arch_pif_4578_stage table.
),
SQ_EDW_TABLES_Bal_Reins_Txn AS (
	SELECT claim_occurrence.claim_occurrence_key AS EDW_CO_key,
	       Count(*)                              AS EDW_COUNT_of_Reins_Transactions,
	       Sum(claim_reins_trans_amt)            AS EDW_SUM_Reins_Trans_Amt
	FROM   @{pipeline().parameters.DB_NAME_EDW}.dbo.claim_reinsurance_transaction AS claim_reinsurance_transaction
	       INNER JOIN @{pipeline().parameters.DB_NAME_EDW}.dbo.claimant_coverage_detail AS claimant_coverage_detail
	         ON claim_reinsurance_transaction.claimant_cov_det_ak_id = claimant_coverage_detail.claimant_cov_det_ak_id
	       INNER JOIN @{pipeline().parameters.DB_NAME_EDW}.dbo.claim_party_occurrence AS claim_party_occurrence
	         ON claimant_coverage_detail.claim_party_occurrence_ak_id = claim_party_occurrence.claim_party_occurrence_ak_id
	       INNER JOIN @{pipeline().parameters.DB_NAME_EDW}.dbo.claim_occurrence AS claim_occurrence
	         ON claim_party_occurrence.claim_occurrence_ak_id = claim_occurrence.claim_occurrence_ak_id
	WHERE  claim_reinsurance_transaction.crrnt_snpsht_flag = 1
	       AND claimant_coverage_detail.crrnt_snpsht_flag = 1
	       AND claim_party_occurrence.crrnt_snpsht_flag = 1
	       AND claim_occurrence.crrnt_snpsht_flag = 1
	       AND claim_reinsurance_transaction.audit_id <= @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	       AND claim_reinsurance_transaction.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	GROUP  BY claim_occurrence_key
),
JNR_EDW_ARCHIVE_Bal_Reins_Txn AS (SELECT
	SQ_EDW_TABLES_Bal_Reins_Txn.EDW_claim_occurrence_key, 
	SQ_EDW_TABLES_Bal_Reins_Txn.EDW_COUNT_of_Reins_Transactions, 
	SQ_EDW_TABLES_Bal_Reins_Txn.EDW_SUM_Reins_Trans_Amt, 
	SQ_ARCH_PIF_4578_STAGE_Bal_Reins_Txn.pif_4578_stage_id AS ARCH_COUNT_of_Reins_Transactions, 
	SQ_ARCH_PIF_4578_STAGE_Bal_Reins_Txn.loss_rec_length AS ARCH_claim_occurrence_key, 
	SQ_ARCH_PIF_4578_STAGE_Bal_Reins_Txn.loss_location_number AS ARCH_SUM_Reins_Trans_Amt
	FROM SQ_EDW_TABLES_Bal_Reins_Txn
	RIGHT OUTER JOIN SQ_ARCH_PIF_4578_STAGE_Bal_Reins_Txn
	ON SQ_ARCH_PIF_4578_STAGE_Bal_Reins_Txn.loss_rec_length = SQ_EDW_TABLES_Bal_Reins_Txn.EDW_claim_occurrence_key
),
EXP_Evaluate_Bal_Reins_Txn AS (
	SELECT
	EDW_claim_occurrence_key,
	EDW_COUNT_of_Reins_Transactions,
	EDW_SUM_Reins_Trans_Amt,
	ARCH_COUNT_of_Reins_Transactions,
	ARCH_claim_occurrence_key,
	ARCH_SUM_Reins_Trans_Amt,
	-- *INF*: IIF(ARCH_SUM_Reins_Trans_Amt = EDW_SUM_Reins_Trans_Amt,'Y','N')
	IFF(ARCH_SUM_Reins_Trans_Amt = EDW_SUM_Reins_Trans_Amt, 'Y', 'N') AS v_Balance_Amount,
	-- *INF*: IIF(v_Balance_Amount = 'Y', 1, -1)
	IFF(v_Balance_Amount = 'Y', 1, - 1) AS err_flag_change,
	err_flag_change AS out_err_flag_reins_txn
	FROM JNR_EDW_ARCHIVE_Bal_Reins_Txn
),
FIL_Err_Flag_Bal_Reins_Txn AS (
	SELECT
	EDW_claim_occurrence_key, 
	out_err_flag_reins_txn
	FROM EXP_Evaluate_Bal_Reins_Txn
	WHERE TRUE
),
LKP_Claim_Occurrence_id_reins AS (
	SELECT
	claim_occurrence_id,
	claim_occurrence_key
	FROM (
		SELECT 
		a.claim_occurrence_id as claim_occurrence_id, 
		a.claim_occurrence_key as claim_occurrence_key 
		FROM claim_occurrence a
		WHERE crrnt_snpsht_flag =1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_occurrence_key ORDER BY claim_occurrence_id) = 1
),
UPD_Claim_Occurrence_Err_Flag_reins_txn AS (
	SELECT
	LKP_Claim_Occurrence_id_reins.claim_occurrence_id, 
	FIL_Err_Flag_Bal_Reins_Txn.out_err_flag_reins_txn
	FROM FIL_Err_Flag_Bal_Reins_Txn
	LEFT JOIN LKP_Claim_Occurrence_id_reins
	ON LKP_Claim_Occurrence_id_reins.claim_occurrence_key = FIL_Err_Flag_Bal_Reins_Txn.EDW_claim_occurrence_key
),
claim_occurrence_update_err_flag_bal_reins AS (
	MERGE INTO claim_occurrence AS T
	USING UPD_Claim_Occurrence_Err_Flag_reins_txn AS S
	ON T.claim_occurrence_id = S.claim_occurrence_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.err_flag_bal_reins = S.out_err_flag_reins_txn
),