WITH
SQ_EDW_TABLES_Balance_Transaction AS (
	SELECT claim_occurrence.claim_occurrence_key AS EDW_CO_key,
	       Count(*)                              AS EDW_COUNT_of_Transactions,
	       Sum(trans_amt)                        AS EDW_SUM_Trans_Amt
	FROM   @{pipeline().parameters.DB_NAME_EDW}.dbo.claim_transaction AS claim_transaction
	       INNER JOIN @{pipeline().parameters.DB_NAME_EDW}.dbo.claimant_coverage_detail AS claimant_coverage_detail
	         ON claim_transaction.claimant_cov_det_ak_id = claimant_coverage_detail.claimant_cov_det_ak_id
	       INNER JOIN @{pipeline().parameters.DB_NAME_EDW}.dbo.claim_party_occurrence AS claim_party_occurrence
	         ON claimant_coverage_detail.claim_party_occurrence_ak_id = claim_party_occurrence.claim_party_occurrence_ak_id
	       INNER JOIN @{pipeline().parameters.DB_NAME_EDW}.dbo.claim_occurrence AS claim_occurrence
	         ON claim_party_occurrence.claim_occurrence_ak_id = claim_occurrence.claim_occurrence_ak_id
	WHERE  claim_transaction.crrnt_snpsht_flag = 1
	       AND claimant_coverage_detail.crrnt_snpsht_flag = 1
	       AND claim_party_occurrence.crrnt_snpsht_flag = 1
	       AND claim_occurrence.crrnt_snpsht_flag = 1
	       AND claim_transaction.trans_offset_onset_ind IN ('N','N/A')
	       AND claim_transaction.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	GROUP  BY claim_occurrence_key 
	ORDER BY  claim_occurrence_key
	
	-- For balancing, we need to take Only Onset Transaction rows into consideration.
),
SQ_claim_transaction_full_extract_stage AS (
	SELECT 	CTX_CLAIM_NBR    AS CTX_CLAIM_NBR,
			COUNT(*)         AS CTX_OBJECT_SEQ_NBR,
			SUM(CTX_TRS_AMT) AS CTX_TRS_AMT
	FROM   WC_STAGE.DBO.CLAIM_TRANSACTION_FULL_EXTRACT_STAGE
	GROUP  BY CTX_CLAIM_NBR
	ORDER  BY CTX_CLAIM_NBR
),
JNR_EDW_STAGE AS (SELECT
	SQ_EDW_TABLES_Balance_Transaction.EDW_claim_occurrence_key, 
	SQ_EDW_TABLES_Balance_Transaction.EDW_COUNT_of_Transactions, 
	SQ_EDW_TABLES_Balance_Transaction.EDW_SUM_Trans_Amt, 
	SQ_claim_transaction_full_extract_stage.STG_COUNT_of_Transactions, 
	SQ_claim_transaction_full_extract_stage.STG_claim_occurrence_key, 
	SQ_claim_transaction_full_extract_stage.STG_SUM_Trans_Amt
	FROM SQ_EDW_TABLES_Balance_Transaction
	RIGHT OUTER JOIN SQ_claim_transaction_full_extract_stage
	ON SQ_claim_transaction_full_extract_stage.STG_claim_occurrence_key = SQ_EDW_TABLES_Balance_Transaction.EDW_claim_occurrence_key
),
EXP_Evaluate AS (
	SELECT
	EDW_claim_occurrence_key,
	EDW_COUNT_of_Transactions,
	EDW_SUM_Trans_Amt,
	STG_COUNT_of_Transactions,
	STG_claim_occurrence_key,
	STG_SUM_Trans_Amt,
	-- *INF*: IIF(STG_SUM_Trans_Amt = EDW_SUM_Trans_Amt,'Y','N')
	IFF(STG_SUM_Trans_Amt = EDW_SUM_Trans_Amt, 'Y', 'N') AS v_Balance_Amount,
	-- *INF*: DECODE(TRUE,
	-- ROUND(abs(EDW_SUM_Trans_Amt - STG_SUM_Trans_Amt),2) > 0.01 , -3,
	-- ROUND(abs(EDW_SUM_Trans_Amt - STG_SUM_Trans_Amt),2) <= 0.01  
	-- AND EDW_COUNT_of_Transactions  != STG_COUNT_of_Transactions, -4,
	-- 0)
	-- 
	-- 
	-- 
	-- --DECODE(TRUE,
	-- --EDW_SUM_Trans_Amt <> STG_SUM_Trans_Amt , -3,
	-- --EDW_SUM_Trans_Amt  = STG_SUM_Trans_Amt 
	-- --  AND EDW_COUNT_of_Transactions  != STG_COUNT_of_Transactions, -4,
	-- --0)
	-- 
	-- --IIF(v_Balance_Amount = 'Y', 1, -2)
	-- 
	--  ---- (-1,-2) is used to identify the PMS claims that are not balancing either by amount or no. of transactions.
	--  ---- (-3,-4) is used to identify EXCEED claims that are not balancing either by amount or no. of transactions.
	DECODE(TRUE,
		ROUND(abs(EDW_SUM_Trans_Amt - STG_SUM_Trans_Amt), 2) > 0.01, - 3,
		ROUND(abs(EDW_SUM_Trans_Amt - STG_SUM_Trans_Amt), 2) <= 0.01 AND EDW_COUNT_of_Transactions != STG_COUNT_of_Transactions, - 4,
		0) AS err_flag_change,
	err_flag_change AS out_err_flag_bal_txn
	FROM JNR_EDW_STAGE
),
LKP_Claim_Occurrence_id AS (
	SELECT
	claim_occurrence_id,
	err_flag_bal_txn,
	claim_occurrence_key
	FROM (
		SELECT 
		a.claim_occurrence_id as claim_occurrence_id, 
		a.err_flag_bal_txn as err_flag_bal_txn,
		a.claim_occurrence_key as claim_occurrence_key 
		FROM 
		dbo.claim_occurrence a
		WHERE 
		source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
		and 
		crrnt_snpsht_flag =1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_occurrence_key ORDER BY claim_occurrence_id DESC) = 1
),
EXP_UpdateFlag AS (
	SELECT
	LKP_Claim_Occurrence_id.claim_occurrence_id AS lkp_claim_occurrence_id,
	LKP_Claim_Occurrence_id.err_flag_bal_txn AS lkp_err_flag_bal_txn,
	EXP_Evaluate.out_err_flag_bal_txn,
	-- *INF*: IIF(out_err_flag_bal_txn = lkp_err_flag_bal_txn, 'NOUPDATE', 'UPDATE')
	IFF(out_err_flag_bal_txn = lkp_err_flag_bal_txn, 'NOUPDATE', 'UPDATE') AS v_update_flag,
	v_update_flag AS update_flag
	FROM EXP_Evaluate
	LEFT JOIN LKP_Claim_Occurrence_id
	ON LKP_Claim_Occurrence_id.claim_occurrence_key = EXP_Evaluate.EDW_claim_occurrence_key
),
FIL_Err_Flag AS (
	SELECT
	lkp_claim_occurrence_id AS claim_occurrence_id, 
	out_err_flag_bal_txn, 
	update_flag
	FROM EXP_UpdateFlag
	WHERE update_flag =  'UPDATE'
),
UPD_Claim_Occurrence_Err_Flag_bal_txn AS (
	SELECT
	claim_occurrence_id, 
	out_err_flag_bal_txn
	FROM FIL_Err_Flag
),
claim_occurrence_update_err_flag_bal_txn AS (
	MERGE INTO claim_occurrence AS T
	USING UPD_Claim_Occurrence_Err_Flag_bal_txn AS S
	ON T.claim_occurrence_id = S.claim_occurrence_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.err_flag_bal_txn = S.out_err_flag_bal_txn
),