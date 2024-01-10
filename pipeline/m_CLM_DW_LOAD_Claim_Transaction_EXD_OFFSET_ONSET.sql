WITH
LKP_sup_convert_s3p_claim_transaction_code AS (
	SELECT
	edw_trans_code,
	s3p_financial_type_code,
	s3p_trans_code,
	s3p_trans_ctgry_code
	FROM (
		SELECT sup_convert_s3p_claim_transaction_code.edw_financial_type_code as edw_financial_type_code, sup_convert_s3p_claim_transaction_code.edw_trans_code as edw_trans_code, sup_convert_s3p_claim_transaction_code.edw_trans_ctgry_code as edw_trans_ctgry_code, sup_convert_s3p_claim_transaction_code.s3p_financial_type_code as s3p_financial_type_code, sup_convert_s3p_claim_transaction_code.s3p_trans_code as s3p_trans_code, sup_convert_s3p_claim_transaction_code.s3p_trans_ctgry_code as s3p_trans_ctgry_code 
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_convert_s3p_claim_transaction_code
		WHERE crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY s3p_financial_type_code,s3p_trans_code,s3p_trans_ctgry_code ORDER BY edw_trans_code) = 1
),
LKP_sup_convert_pms_claim_transaction_code AS (
	SELECT
	edw_trans_code,
	pms_trans_code
	FROM (
		SELECT 
			edw_trans_code,
			pms_trans_code
		FROM sup_convert_pms_claim_transaction_code
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pms_trans_code ORDER BY edw_trans_code) = 1
),
LKP_Sup_Claim_Transaction_Code AS (
	SELECT
	sup_claim_trans_code_id,
	trans_code
	FROM (
		SELECT 
			sup_claim_trans_code_id,
			trans_code
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_claim_transaction_code
		WHERE crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY trans_code ORDER BY sup_claim_trans_code_id) = 1
),
SEQ_Claim_Transaction AS (
	CREATE SEQUENCE SEQ_Claim_Transaction
	START = 0
	INCREMENT = 1;
),
SQ_Stage_Tables AS (
	SELECT
	 tch_claim_nbr,
	 MAX(off_onset_ts) as off_onset_ts,
	 off_client_id 
	FROM (
	SELECT 
	 A.tch_claim_nbr,
	 A.off_onset_ts as off_onset_ts,
	 B.off_client_id 
	 FROM 
	        clm_offset_onset_stage A,
	        offset_onset_cov_stage B,
	        offset_onset_unit_stage C
	 WHERE A.tch_claim_nbr = B.tch_claim_nbr
	        AND B.tch_claim_nbr = C.tch_claim_nbr
	        AND A.off_onset_ts = B.off_onset_ts
	        AND B.off_onset_ts = C.off_onset_ts
	         AND A.off_onset_ts > = '@{pipeline().parameters.SELECTION_START_TS}'
	        AND B.off_onset_ts > =  '@{pipeline().parameters.SELECTION_START_TS}'
	
	union all
	
	SELECT 
	A.ClaimOccurrenceKey as tch_claim_nbr,
	A.ClaimUpdateDate as off_onset_ts,
	A.ClaimantPartyKey as off_client_id 
	FROM @{pipeline().parameters.EDW_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkClaimCatastropheCodeOnsetOffset A
	join @{pipeline().parameters.EDW_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_occurrence CO
	on A.ClaimOccurrenceAKID=CO.claim_occurrence_ak_id and CO.crrnt_snpsht_flag=1
	where A.ClaimUpdateDate>='@{pipeline().parameters.SELECTION_START_TS}'
	) Src
	group by tch_claim_nbr, off_client_id
),
EXP_Default AS (
	SELECT
	tch_claim_nbr,
	off_onset_ts,
	off_client_id
	FROM SQ_Stage_Tables
),
LKP_Claim_Party_Occurrence_AK_ID AS (
	SELECT
	claim_party_occurrence_ak_id,
	claim_occurrence_ak_id,
	claim_occurrence_type_code,
	s3p_claim_num,
	claimant_num,
	claim_party_role_code
	FROM (
		SELECT CPO.claim_party_occurrence_ak_id      AS claim_party_occurrence_ak_id,
		              CO.claim_occurrence_ak_id                         AS claim_occurrence_ak_id, 
		       CO.claim_occurrence_type_code         AS claim_occurrence_type_code,
		       CO.s3p_claim_num                                     AS s3p_claim_num,
		       LTRIM(RTRIM(CO.claim_occurrence_key)) AS claimant_num,
		       LTRIM(RTRIM(CP.claim_party_key))      AS claim_party_role_code
		FROM   @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_party_occurrence CPO,
		       @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_party CP,
		       @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_occurrence CO
		WHERE  CO.claim_occurrence_ak_id = CPO.claim_occurrence_ak_id
		       AND CP.claim_party_ak_id = CPO.claim_party_ak_id
		       AND CO.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
		       AND CP.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
		       AND CPO.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
		       AND CPO.claim_party_role_code = 'CLMT'
		       AND CO.crrnt_snpsht_flag = 1
		       AND CP.crrnt_snpsht_flag = 1
		       AND CPO.crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claimant_num,claim_party_role_code ORDER BY claim_party_occurrence_ak_id DESC) = 1
),
EXP_Lkp_Values AS (
	SELECT
	LKP_Claim_Party_Occurrence_AK_ID.claim_party_occurrence_ak_id,
	EXP_Default.tch_claim_nbr,
	EXP_Default.off_onset_ts,
	EXP_Default.off_client_id,
	LKP_Claim_Party_Occurrence_AK_ID.claim_occurrence_ak_id,
	LKP_Claim_Party_Occurrence_AK_ID.claim_occurrence_type_code,
	LKP_Claim_Party_Occurrence_AK_ID.s3p_claim_num
	FROM EXP_Default
	LEFT JOIN LKP_Claim_Party_Occurrence_AK_ID
	ON LKP_Claim_Party_Occurrence_AK_ID.claimant_num = EXP_Default.tch_claim_nbr AND LKP_Claim_Party_Occurrence_AK_ID.claim_party_role_code = EXP_Default.off_client_id
),
SQL_Claim_Transaction AS (-- SQL_Claim_Transaction

	##############################################

	# TODO: Place holder for Custom transformation

	##############################################
),
EXP_Pass_Through AS (
	SELECT
	claim_trans_id,
	claim_trans_ak_id,
	claimant_cov_det_ak_id,
	claim_pay_ak_id,
	cause_of_loss,
	reserve_ctgry,
	type_disability,
	sar_id,
	offset_onset_ind,
	financial_type_code,
	s3p_trans_code,
	pms_trans_code,
	trans_code,
	off_onset_ts_output,
	trans_date,
	s3p_updated_date,
	s3p_to_pms_trans_date,
	pms_acct_entered_date,
	trans_base_type_code,
	trans_ctgry_code,
	trans_amt,
	trans_hist_amt,
	trans_rsn,
	draft_num,
	single_check_ind,
	offset_reissue_ind,
	reprocess_date,
	trans_entry_oper_id,
	wc_stage_pk_id,
	err_flag,
	crrnt_snpsht_flag,
	audit_id,
	eff_from_date,
	eff_to_date,
	source_sys_id,
	created_date,
	modified_date,
	tax_id,
	claim_master_1099_list_ak_id,
	s3p_created_date,
	tch_claim_nbr_output AS tch_claim_nbr,
	off_client_id_output AS off_client_id,
	CauseOfLossID,
	SupReserveCategoryCodeID,
	FinancialTypeCodeID,
	S3PTransactionCodeID,
	PMSTransactionCodeID,
	TransactionCodeID,
	SupTransactionCategoryCodeID
	FROM SQL_Claim_Transaction
),
RTR_Offset_Onset AS (
	SELECT
	claim_trans_id,
	claim_trans_ak_id,
	claimant_cov_det_ak_id,
	claim_pay_ak_id,
	cause_of_loss,
	reserve_ctgry,
	type_disability,
	sar_id,
	offset_onset_ind,
	financial_type_code,
	s3p_trans_code,
	pms_trans_code,
	trans_code,
	off_onset_ts_output,
	trans_date,
	s3p_updated_date,
	s3p_to_pms_trans_date,
	pms_acct_entered_date,
	trans_base_type_code,
	trans_ctgry_code,
	trans_amt,
	trans_hist_amt,
	trans_rsn,
	draft_num,
	single_check_ind,
	offset_reissue_ind,
	reprocess_date,
	trans_entry_oper_id,
	wc_stage_pk_id,
	err_flag,
	crrnt_snpsht_flag,
	audit_id,
	eff_from_date,
	eff_to_date,
	source_sys_id,
	created_date,
	modified_date,
	tax_id,
	claim_master_1099_list_ak_id,
	s3p_created_date,
	CauseOfLossID,
	SupReserveCategoryCodeID,
	FinancialTypeCodeID,
	S3PTransactionCodeID,
	PMSTransactionCodeID,
	TransactionCodeID,
	SupTransactionCategoryCodeID
	FROM EXP_Pass_Through
),
RTR_Offset_Onset_OFFSET_UPDATE AS (SELECT * FROM RTR_Offset_Onset WHERE IN(pms_trans_code,'90','92','40','41','42','43', '65', '66','91','21','22','23','24','26','27','28','29','37','71','72','73','75','76','77','78','79','81','82','83','84','86','87','88','89')


---Add trans_code 77,78,79,'87' after defect was found.),
RTR_Offset_Onset_ONSET_INSERT AS (SELECT * FROM RTR_Offset_Onset WHERE IN(pms_trans_code,'21','22','23','24','26','27','28','29','37','40','41','42','43','65','66','71','72','73','75','76','77','78'
,'79','81','82','83','84', '86','87','88','89','90','91','92','95','97','98','99')),
RTR_Offset_Onset_OFFSET_INSERT AS (SELECT * FROM RTR_Offset_Onset WHERE IN(pms_trans_code,'90','92','21','22','23','24','26','27','28','29','37','71','72','73','75','76','81','82','83','84','86','88','89','77')



--- IN ('90','92',) and not in ('95','97','98','99') and not in ('43','65','66','40','41','42')
----IN(pms_trans_code,'21','22','23','24','26','27','28','29','37','40','41','42','43','65','66','71','72','73','75','76',
----'77','78','79','81','82','83','84','86','87','88','89','90','91','92','95','97','98','99')
---- '87' is only code missing from above logic and Onset Offset PMS program doesnt talk about this code.),
RTR_Offset_Onset_OFFSET_UPDATE_NONREG_RES AS (SELECT * FROM RTR_Offset_Onset WHERE IN(pms_trans_code,'95','97','98','99')


---('21','22','23','24','26','27','28','29','37','40','41','42','43','65','66','71','72','73','75','76','77','78','79',
----'81','82','83','84','86','87','88','89','90','91','92','95','97','98','99')),
EXP_Offset_Insert AS (
	SELECT
	claim_trans_id,
	claim_trans_ak_id,
	claimant_cov_det_ak_id,
	claim_pay_ak_id,
	cause_of_loss,
	reserve_ctgry,
	type_disability,
	sar_id,
	offset_onset_ind,
	'O' AS trans_offset_onset_ind,
	financial_type_code,
	s3p_trans_code,
	'N/A' AS s3p_trans_code_out,
	pms_trans_code,
	-- *INF*: DECODE(TRUE,IN(pms_trans_code,'90','92'),'41',
	--                                     IN(pms_trans_code,'21','22','23','24','25','28','29'),'29',
	--                                     IN(pms_trans_code,'26','27','37'),'26',
	--                                     IN(pms_trans_code,'81','83','88'),'88',
	--                                     IN(pms_trans_code,'82','84','89'),'89',
	--                                     IN(pms_trans_code,'74','75','76'),'76',
	--                                     pms_trans_code='86','87',
	--                                     pms_trans_code='71','78',
	--                                     pms_trans_code='72','79',
	--                                     pms_trans_code='73','77',
	--                                     pms_trans_code)
	-- 
	-- 
	-- 
	DECODE(TRUE,
		IN(pms_trans_code, '90', '92'), '41',
		IN(pms_trans_code, '21', '22', '23', '24', '25', '28', '29'), '29',
		IN(pms_trans_code, '26', '27', '37'), '26',
		IN(pms_trans_code, '81', '83', '88'), '88',
		IN(pms_trans_code, '82', '84', '89'), '89',
		IN(pms_trans_code, '74', '75', '76'), '76',
		pms_trans_code = '86', '87',
		pms_trans_code = '71', '78',
		pms_trans_code = '72', '79',
		pms_trans_code = '73', '77',
		pms_trans_code) AS v_pms_trans_code,
	v_pms_trans_code AS pms_trans_code_out,
	v_pms_trans_code AS lkp_PMSTransactionCodeID,
	trans_code,
	-- *INF*: :LKP.LKP_SUP_CONVERT_PMS_CLAIM_TRANSACTION_CODE(v_pms_trans_code)
	LKP_SUP_CONVERT_PMS_CLAIM_TRANSACTION_CODE_v_pms_trans_code.edw_trans_code AS v_trans_code,
	-- *INF*: IIF(RTRIM(v_trans_code) <> '0', v_trans_code, 
	-- 	DECODE(TRUE, 	IN (v_pms_trans_code,'76','26','88','89'),IIF(trans_hist_amt = 0.0,'39','38'), 
	-- 						IN (v_pms_trans_code,'27','83','84'),IIF(trans_amt = 0.0,'40','30')
	--  					)
	-- )
	-- 
	-- ---- Above rules are for Claim_Transaction_PMS mapping. Used these rules as logic is based on pms_trans_code.
	IFF(RTRIM(v_trans_code) <> '0', v_trans_code, DECODE(TRUE,
		IN(v_pms_trans_code, '76', '26', '88', '89'), IFF(trans_hist_amt = 0.0, '39', '38'),
		IN(v_pms_trans_code, '27', '83', '84'), IFF(trans_amt = 0.0, '40', '30'))) AS trans_code_out,
	off_onset_ts_output,
	trans_date,
	-- *INF*: IIF(v_pms_trans_code = '41',off_onset_ts_output,trans_date)
	IFF(v_pms_trans_code = '41', off_onset_ts_output, trans_date) AS trans_date_Out,
	s3p_updated_date,
	s3p_to_pms_trans_date,
	pms_acct_entered_date,
	off_onset_ts_output AS pms_acct_entered_date_Out,
	trans_base_type_code,
	trans_ctgry_code,
	trans_amt,
	-- *INF*: IIF(v_pms_trans_code= '41', 0.0, -1* trans_amt)
	IFF(v_pms_trans_code = '41', 0.0, - 1 * trans_amt) AS trans_amt_Out,
	trans_hist_amt,
	-- *INF*: IIF(v_pms_trans_code= '41', -1* trans_amt,-1 * trans_hist_amt )
	IFF(v_pms_trans_code = '41', - 1 * trans_amt, - 1 * trans_hist_amt) AS trans_hist_amt_Out,
	trans_rsn,
	draft_num,
	single_check_ind,
	offset_reissue_ind,
	reprocess_date,
	trans_entry_oper_id,
	wc_stage_pk_id,
	-1 * wc_stage_pk_id AS wc_stage_pk_id_Out,
	err_flag,
	crrnt_snpsht_flag,
	audit_id,
	eff_from_date,
	eff_to_date,
	source_sys_id,
	created_date,
	modified_date,
	tax_id,
	claim_master_1099_list_ak_id,
	s3p_created_date,
	CauseOfLossID AS CauseOfLossID4,
	SupReserveCategoryCodeID AS SupReserveCategoryCodeID4,
	FinancialTypeCodeID AS FinancialTypeCodeID4,
	S3PTransactionCodeID AS S3PTransactionCodeID4,
	PMSTransactionCodeID AS PMSTransactionCodeID4,
	TransactionCodeID AS TransactionCodeID4,
	SupTransactionCategoryCodeID AS SupTransactionCategoryCodeID4
	FROM RTR_Offset_Onset_OFFSET_INSERT
	LEFT JOIN LKP_SUP_CONVERT_PMS_CLAIM_TRANSACTION_CODE LKP_SUP_CONVERT_PMS_CLAIM_TRANSACTION_CODE_v_pms_trans_code
	ON LKP_SUP_CONVERT_PMS_CLAIM_TRANSACTION_CODE_v_pms_trans_code.pms_trans_code = v_pms_trans_code

),
EXP_Onset_Insert AS (
	SELECT
	claim_trans_id,
	claim_trans_ak_id,
	claimant_cov_det_ak_id,
	claim_pay_ak_id,
	cause_of_loss,
	reserve_ctgry,
	type_disability,
	sar_id,
	offset_onset_ind,
	financial_type_code,
	s3p_trans_code,
	pms_trans_code,
	trans_code,
	'N' AS trans_offset_onset_ind,
	off_onset_ts_output,
	trans_date,
	s3p_updated_date,
	s3p_to_pms_trans_date,
	pms_acct_entered_date,
	-- *INF*: IIF(IN(pms_trans_code,'90','92','95','97','98','99','43','65','66','91'),pms_acct_entered_date,off_onset_ts_output)
	IFF(IN(pms_trans_code, '90', '92', '95', '97', '98', '99', '43', '65', '66', '91'), pms_acct_entered_date, off_onset_ts_output) AS pms_acct_entered_date_Out,
	trans_base_type_code,
	trans_ctgry_code,
	trans_amt,
	trans_hist_amt,
	trans_rsn,
	draft_num,
	single_check_ind,
	offset_reissue_ind,
	reprocess_date,
	trans_entry_oper_id,
	wc_stage_pk_id,
	err_flag,
	crrnt_snpsht_flag,
	eff_from_date,
	eff_to_date,
	source_sys_id,
	created_date,
	modified_date,
	tax_id,
	claim_master_1099_list_ak_id,
	s3p_created_date,
	CauseOfLossID AS CauseOfLossID3,
	SupReserveCategoryCodeID AS SupReserveCategoryCodeID3,
	FinancialTypeCodeID AS FinancialTypeCodeID3,
	S3PTransactionCodeID AS S3PTransactionCodeID3,
	PMSTransactionCodeID AS PMSTransactionCodeID3,
	TransactionCodeID AS TransactionCodeID3,
	SupTransactionCategoryCodeID AS SupTransactionCategoryCodeID3
	FROM RTR_Offset_Onset_ONSET_INSERT
),
Union_Offset_Onset AS (
	SELECT claim_trans_id, claim_trans_ak_id, claimant_cov_det_ak_id, claim_pay_ak_id, cause_of_loss, reserve_ctgry, type_disability, sar_id, offset_onset_ind, financial_type_code, s3p_trans_code, pms_trans_code, trans_code, trans_date, s3p_updated_date, s3p_to_pms_trans_date, pms_acct_entered_date_Out AS pms_acct_entered_date, trans_base_type_code, trans_ctgry_code, trans_amt, trans_hist_amt, trans_rsn, draft_num, single_check_ind, offset_reissue_ind, reprocess_date, trans_entry_oper_id, wc_stage_pk_id, err_flag, crrnt_snpsht_flag, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date, tax_id, claim_master_1099_list_ak_id, trans_offset_onset_ind, s3p_created_date, CauseOfLossID3 AS CauseOfLossID, SupReserveCategoryCodeID3 AS SupReserveCategoryCodeID, FinancialTypeCodeID3 AS FinancialTypeCodeID, S3PTransactionCodeID3 AS S3PTransactionCodeID, PMSTransactionCodeID3 AS PMSTransactionCodeID, TransactionCodeID3 AS TransactionCodeID, SupTransactionCategoryCodeID3 AS SupTransactionCategoryCodeID
	FROM EXP_Onset_Insert
	UNION
	SELECT claim_trans_id, claim_trans_ak_id, claimant_cov_det_ak_id, claim_pay_ak_id, cause_of_loss, reserve_ctgry, type_disability, sar_id, offset_onset_ind, financial_type_code, s3p_trans_code_out AS s3p_trans_code, pms_trans_code_out AS pms_trans_code, trans_code_out AS trans_code, trans_date_Out AS trans_date, s3p_updated_date, s3p_to_pms_trans_date, pms_acct_entered_date_Out AS pms_acct_entered_date, trans_base_type_code, trans_ctgry_code, trans_amt_Out AS trans_amt, trans_hist_amt_Out AS trans_hist_amt, trans_rsn, draft_num, single_check_ind, offset_reissue_ind, reprocess_date, trans_entry_oper_id, wc_stage_pk_id_Out AS wc_stage_pk_id, err_flag, crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date, tax_id, claim_master_1099_list_ak_id, trans_offset_onset_ind, s3p_created_date, CauseOfLossID4 AS CauseOfLossID, SupReserveCategoryCodeID4 AS SupReserveCategoryCodeID, FinancialTypeCodeID4 AS FinancialTypeCodeID, S3PTransactionCodeID4 AS S3PTransactionCodeID, PMSTransactionCodeID4 AS PMSTransactionCodeID, TransactionCodeID4 AS TransactionCodeID, SupTransactionCategoryCodeID4 AS SupTransactionCategoryCodeID
	FROM EXP_Offset_Insert
),
EXP_Claim_Trans_AK_ID AS (
	SELECT
	SEQ_Claim_Transaction.NEXTVAL,
	NEXTVAL AS Claim_trans_ak_id,
	claimant_cov_det_ak_id,
	claim_pay_ak_id,
	cause_of_loss,
	reserve_ctgry,
	type_disability,
	sar_id,
	offset_onset_ind AS offset_onset_ind_Out,
	financial_type_code,
	s3p_trans_code,
	pms_trans_code,
	trans_code,
	trans_date,
	s3p_updated_date,
	s3p_to_pms_trans_date,
	pms_acct_entered_date,
	trans_base_type_code,
	trans_ctgry_code,
	trans_amt,
	trans_hist_amt,
	trans_rsn,
	draft_num,
	single_check_ind,
	offset_reissue_ind,
	reprocess_date,
	trans_entry_oper_id,
	wc_stage_pk_id,
	err_flag,
	crrnt_snpsht_flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	-- *INF*: TO_DATE('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS') AS eff_from_date,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS eff_to_date,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS source_sys_id,
	tax_id,
	claim_master_1099_list_ak_id,
	SYSDATE AS Created_Date,
	trans_offset_onset_ind,
	s3p_created_date,
	CauseOfLossID,
	SupReserveCategoryCodeID,
	FinancialTypeCodeID,
	S3PTransactionCodeID,
	PMSTransactionCodeID,
	TransactionCodeID,
	SupTransactionCategoryCodeID
	FROM Union_Offset_Onset
),
claim_transaction_Insert AS (
	INSERT INTO claim_transaction
	(claim_trans_ak_id, claimant_cov_det_ak_id, claim_pay_ak_id, cause_of_loss, reserve_ctgry, type_disability, sar_id, offset_onset_ind, financial_type_code, s3p_trans_code, pms_trans_code, trans_code, trans_date, s3p_updated_date, s3p_to_pms_trans_date, pms_acct_entered_date, trans_base_type_code, trans_ctgry_code, trans_amt, trans_hist_amt, trans_rsn, draft_num, single_check_ind, offset_reissue_ind, reprocess_date, trans_entry_oper_id, wc_stage_pk_id, err_flag, crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date, tax_id, claim_master_1099_list_ak_id, trans_offset_onset_ind, s3p_created_date, CauseOfLossID, SupReserveCategoryCodeID, FinancialTypeCodeID, S3PTransactionCodeID, PMSTransactionCodeID, TransactionCodeID, SupTransactionCategoryCodeID)
	SELECT 
	Claim_trans_ak_id AS CLAIM_TRANS_AK_ID, 
	CLAIMANT_COV_DET_AK_ID, 
	CLAIM_PAY_AK_ID, 
	CAUSE_OF_LOSS, 
	RESERVE_CTGRY, 
	TYPE_DISABILITY, 
	SAR_ID, 
	offset_onset_ind_Out AS OFFSET_ONSET_IND, 
	FINANCIAL_TYPE_CODE, 
	S3P_TRANS_CODE, 
	PMS_TRANS_CODE, 
	TRANS_CODE, 
	TRANS_DATE, 
	S3P_UPDATED_DATE, 
	S3P_TO_PMS_TRANS_DATE, 
	PMS_ACCT_ENTERED_DATE, 
	TRANS_BASE_TYPE_CODE, 
	TRANS_CTGRY_CODE, 
	TRANS_AMT, 
	TRANS_HIST_AMT, 
	TRANS_RSN, 
	DRAFT_NUM, 
	SINGLE_CHECK_IND, 
	OFFSET_REISSUE_IND, 
	REPROCESS_DATE, 
	TRANS_ENTRY_OPER_ID, 
	WC_STAGE_PK_ID, 
	ERR_FLAG, 
	CRRNT_SNPSHT_FLAG, 
	AUDIT_ID, 
	EFF_FROM_DATE, 
	EFF_TO_DATE, 
	SOURCE_SYS_ID, 
	Created_Date AS CREATED_DATE, 
	Created_Date AS MODIFIED_DATE, 
	TAX_ID, 
	CLAIM_MASTER_1099_LIST_AK_ID, 
	TRANS_OFFSET_ONSET_IND, 
	S3P_CREATED_DATE, 
	CAUSEOFLOSSID, 
	SUPRESERVECATEGORYCODEID, 
	FINANCIALTYPECODEID, 
	S3PTRANSACTIONCODEID, 
	PMSTRANSACTIONCODEID, 
	TRANSACTIONCODEID, 
	SUPTRANSACTIONCATEGORYCODEID
	FROM EXP_Claim_Trans_AK_ID
),
EXP_Offset_Update AS (
	SELECT
	claim_trans_id,
	claim_trans_ak_id,
	claimant_cov_det_ak_id,
	claim_pay_ak_id,
	cause_of_loss,
	reserve_ctgry,
	type_disability,
	sar_id,
	offset_onset_ind,
	'O' AS trans_offset_onset_ind,
	financial_type_code,
	s3p_trans_code,
	-- *INF*: IIF(IN(s3p_trans_code,'90','92'),'91',s3p_trans_code)
	IFF(IN(s3p_trans_code, '90', '92'), '91', s3p_trans_code) AS v_s3p_trans_code,
	v_s3p_trans_code AS o_s3p_trans_code,
	-- *INF*: :LKP.LKP_SUP_CLAIM_TRANSACTION_CODE(v_s3p_trans_code)
	LKP_SUP_CLAIM_TRANSACTION_CODE_v_s3p_trans_code.sup_claim_trans_code_id AS lkp_S3PTransactionCodeID,
	pms_trans_code,
	-- *INF*: IIF(IN(pms_trans_code,'90','92'),'91',pms_trans_code)
	IFF(IN(pms_trans_code, '90', '92'), '91', pms_trans_code) AS v_pms_trans_code,
	v_pms_trans_code AS o_pms_trans_code,
	-- *INF*: :LKP.LKP_SUP_CLAIM_TRANSACTION_CODE(v_pms_trans_code)
	LKP_SUP_CLAIM_TRANSACTION_CODE_v_pms_trans_code.sup_claim_trans_code_id AS lkp_PMSTransactionCodeID,
	trans_code,
	off_onset_ts_output,
	-- *INF*: IIF(IN(trans_code,'90','92'),'91',trans_code)
	IFF(IN(trans_code, '90', '92'), '91', trans_code) AS v_Claim_trans_code,
	v_Claim_trans_code AS o_trans_code,
	-- *INF*: :LKP.LKP_SUP_CLAIM_TRANSACTION_CODE(v_Claim_trans_code)
	LKP_SUP_CLAIM_TRANSACTION_CODE_v_Claim_trans_code.sup_claim_trans_code_id AS lkp_TransactionCodeID,
	trans_date,
	s3p_updated_date,
	s3p_to_pms_trans_date,
	pms_acct_entered_date,
	-- *INF*: GREATEST(trans_date, pms_acct_entered_date)
	GREATEST(trans_date, pms_acct_entered_date) AS v_Greatest_trans_date_pms_acct_entered_date,
	-- *INF*: DECODE(TRUE,
	-- IN(pms_trans_code,'90','92','95','97','98','99','43','65','66','91'),
	-- pms_acct_entered_date,
	-- TRUNC(v_Greatest_trans_date_pms_acct_entered_date,'MM')=TRUNC(off_onset_ts_output, 'MM'),
	-- off_onset_ts_output,
	-- SET_DATE_PART(SET_DATE_PART(SET_DATE_PART(LAST_DAY(v_Greatest_trans_date_pms_acct_entered_date), 'HH24', 23), 'MI', 59), 'SS',59)
	-- )
	DECODE(TRUE,
		IN(pms_trans_code, '90', '92', '95', '97', '98', '99', '43', '65', '66', '91'), pms_acct_entered_date,
		TRUNC(v_Greatest_trans_date_pms_acct_entered_date, 'MM') = TRUNC(off_onset_ts_output, 'MM'), off_onset_ts_output,
		SET_DATE_PART(SET_DATE_PART(SET_DATE_PART(LAST_DAY(v_Greatest_trans_date_pms_acct_entered_date), 'HH24', 23), 'MI', 59), 'SS', 59)) AS o_pms_acct_entered_date,
	trans_base_type_code,
	trans_ctgry_code,
	trans_amt,
	-- *INF*: IIF(IN(pms_trans_code,'90','92'),0.0,trans_amt)
	IFF(IN(pms_trans_code, '90', '92'), 0.0, trans_amt) AS trans_amt_Out,
	trans_hist_amt,
	trans_rsn,
	draft_num,
	single_check_ind,
	offset_reissue_ind,
	reprocess_date,
	trans_entry_oper_id,
	wc_stage_pk_id,
	-- *INF*: -1 * wc_stage_pk_id
	-- 
	-- 
	-- ----- For Offset record, when we update we want to make that record  have -ve stage pk id 
	- 1 * wc_stage_pk_id AS wc_stage_pk_id_Out,
	err_flag,
	crrnt_snpsht_flag,
	audit_id,
	eff_from_date,
	eff_to_date,
	source_sys_id,
	created_date,
	tax_id,
	claim_master_1099_list_ak_id,
	SYSDATE AS Modified_date,
	FinancialTypeCodeID,
	-- *INF*: IIF(ISNULL(lkp_S3PTransactionCodeID),-1,lkp_S3PTransactionCodeID)
	IFF(lkp_S3PTransactionCodeID IS NULL, - 1, lkp_S3PTransactionCodeID) AS o_S3PTransactionCodeID,
	-- *INF*: IIF(ISNULL(lkp_PMSTransactionCodeID),-1,lkp_PMSTransactionCodeID)
	IFF(lkp_PMSTransactionCodeID IS NULL, - 1, lkp_PMSTransactionCodeID) AS o_PMSTransactionCodeID,
	-- *INF*: IIF(ISNULL(lkp_TransactionCodeID),-1,lkp_TransactionCodeID)
	IFF(lkp_TransactionCodeID IS NULL, - 1, lkp_TransactionCodeID) AS o_TransactionCodeID
	FROM RTR_Offset_Onset_OFFSET_UPDATE
	LEFT JOIN LKP_SUP_CLAIM_TRANSACTION_CODE LKP_SUP_CLAIM_TRANSACTION_CODE_v_s3p_trans_code
	ON LKP_SUP_CLAIM_TRANSACTION_CODE_v_s3p_trans_code.trans_code = v_s3p_trans_code

	LEFT JOIN LKP_SUP_CLAIM_TRANSACTION_CODE LKP_SUP_CLAIM_TRANSACTION_CODE_v_pms_trans_code
	ON LKP_SUP_CLAIM_TRANSACTION_CODE_v_pms_trans_code.trans_code = v_pms_trans_code

	LEFT JOIN LKP_SUP_CLAIM_TRANSACTION_CODE LKP_SUP_CLAIM_TRANSACTION_CODE_v_Claim_trans_code
	ON LKP_SUP_CLAIM_TRANSACTION_CODE_v_Claim_trans_code.trans_code = v_Claim_trans_code

),
UPD_Offset_Update AS (
	SELECT
	claim_trans_id, 
	trans_offset_onset_ind, 
	o_s3p_trans_code, 
	o_pms_trans_code, 
	o_trans_code, 
	o_pms_acct_entered_date, 
	trans_amt_Out, 
	Modified_date, 
	wc_stage_pk_id_Out, 
	FinancialTypeCodeID, 
	o_S3PTransactionCodeID, 
	o_PMSTransactionCodeID, 
	o_TransactionCodeID
	FROM EXP_Offset_Update
),
claim_transaction_Update AS (
	MERGE INTO claim_transaction AS T
	USING UPD_Offset_Update AS S
	ON T.claim_trans_id = S.claim_trans_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.s3p_trans_code = S.o_s3p_trans_code, T.pms_trans_code = S.o_pms_trans_code, T.trans_code = S.o_trans_code, T.pms_acct_entered_date = S.o_pms_acct_entered_date, T.trans_amt = S.trans_amt_Out, T.wc_stage_pk_id = S.wc_stage_pk_id_Out, T.modified_date = S.Modified_date, T.trans_offset_onset_ind = S.trans_offset_onset_ind, T.FinancialTypeCodeID = S.FinancialTypeCodeID, T.S3PTransactionCodeID = S.o_S3PTransactionCodeID, T.PMSTransactionCodeID = S.o_PMSTransactionCodeID, T.TransactionCodeID = S.o_TransactionCodeID
),
EXP_Offset_Update_NonReg_Res AS (
	SELECT
	claim_trans_id,
	claim_trans_ak_id,
	claimant_cov_det_ak_id,
	claim_pay_ak_id,
	cause_of_loss,
	reserve_ctgry,
	type_disability,
	sar_id,
	offset_onset_ind,
	'O' AS trans_offset_onset_ind,
	financial_type_code,
	s3p_trans_code,
	pms_trans_code,
	-- *INF*: DECODE(TRUE,pms_trans_code='95','40'
	--                                    ,pms_trans_code='97','27'
	--                                    ,pms_trans_code='98','83'
	--                                    ,pms_trans_code='99','84')
	-- 
	DECODE(TRUE,
		pms_trans_code = '95', '40',
		pms_trans_code = '97', '27',
		pms_trans_code = '98', '83',
		pms_trans_code = '99', '84') AS v_pms_trans_code,
	v_pms_trans_code AS o_pms_trans_code,
	-- *INF*: :LKP.LKP_SUP_CLAIM_TRANSACTION_CODE(v_pms_trans_code)
	LKP_SUP_CLAIM_TRANSACTION_CODE_v_pms_trans_code.sup_claim_trans_code_id AS lkp_PMSTransactionCodeID,
	trans_code,
	-- *INF*: :LKP.LKP_SUP_CONVERT_PMS_CLAIM_TRANSACTION_CODE(v_pms_trans_code)
	LKP_SUP_CONVERT_PMS_CLAIM_TRANSACTION_CODE_v_pms_trans_code.edw_trans_code AS v_trans_code,
	-- *INF*: IIF(RTRIM(v_trans_code) <> '0', v_trans_code, 
	-- 	DECODE(TRUE, 	IN (v_pms_trans_code,'76','26','88','89'),IIF(trans_hist_amt = 0.0,'39','38'), 
	-- 						IN (v_pms_trans_code,'27','83','84'),IIF(trans_amt = 0.0,'40','30')
	--  					)
	-- )
	-- 
	-- --- Above rules are from Claim_Transaction_PMS mapping, as we are using PMS_trans_code, and since we are updating the pms_trans_code in this pipeline from one value to other. Updating the edw_trans_code as well.
	IFF(RTRIM(v_trans_code) <> '0', v_trans_code, DECODE(TRUE,
		IN(v_pms_trans_code, '76', '26', '88', '89'), IFF(trans_hist_amt = 0.0, '39', '38'),
		IN(v_pms_trans_code, '27', '83', '84'), IFF(trans_amt = 0.0, '40', '30'))) AS trans_code_out,
	trans_code_out AS o_claim_trans_code,
	-- *INF*: :LKP.LKP_SUP_CLAIM_TRANSACTION_CODE(v_trans_code)
	LKP_SUP_CLAIM_TRANSACTION_CODE_v_trans_code.sup_claim_trans_code_id AS lkp_claim_trans_code_id,
	off_onset_ts_output,
	trans_date,
	-- *INF*: off_onset_ts_output
	-- 
	-- 
	-- ------ For NonReg Reserve transactions (95,97,98,99), existing transaction data gets updated with the sysdate the transaction gets processed.
	off_onset_ts_output AS trans_date_Out,
	s3p_updated_date,
	s3p_to_pms_trans_date,
	pms_acct_entered_date,
	off_onset_ts_output AS pms_acct_entered_date_Out,
	trans_base_type_code,
	trans_ctgry_code,
	trans_amt,
	0.0 AS trans_amt_Out,
	trans_hist_amt,
	trans_rsn,
	draft_num,
	single_check_ind,
	offset_reissue_ind,
	reprocess_date,
	trans_entry_oper_id,
	wc_stage_pk_id,
	-1 * wc_stage_pk_id AS wc_stage_pk_id_Out,
	err_flag,
	crrnt_snpsht_flag,
	audit_id,
	eff_from_date,
	eff_to_date,
	source_sys_id,
	created_date,
	tax_id,
	claim_master_1099_list_ak_id,
	SYSDATE AS Modified_date,
	-- *INF*: IIF(ISNULL(lkp_PMSTransactionCodeID),-1,lkp_PMSTransactionCodeID)
	IFF(lkp_PMSTransactionCodeID IS NULL, - 1, lkp_PMSTransactionCodeID) AS o_PMSTransactionCodeID,
	-- *INF*: IIF(ISNULL(lkp_claim_trans_code_id),-1,lkp_claim_trans_code_id)
	IFF(lkp_claim_trans_code_id IS NULL, - 1, lkp_claim_trans_code_id) AS o_TransactionCodeID
	FROM RTR_Offset_Onset_OFFSET_UPDATE_NONREG_RES
	LEFT JOIN LKP_SUP_CLAIM_TRANSACTION_CODE LKP_SUP_CLAIM_TRANSACTION_CODE_v_pms_trans_code
	ON LKP_SUP_CLAIM_TRANSACTION_CODE_v_pms_trans_code.trans_code = v_pms_trans_code

	LEFT JOIN LKP_SUP_CONVERT_PMS_CLAIM_TRANSACTION_CODE LKP_SUP_CONVERT_PMS_CLAIM_TRANSACTION_CODE_v_pms_trans_code
	ON LKP_SUP_CONVERT_PMS_CLAIM_TRANSACTION_CODE_v_pms_trans_code.pms_trans_code = v_pms_trans_code

	LEFT JOIN LKP_SUP_CLAIM_TRANSACTION_CODE LKP_SUP_CLAIM_TRANSACTION_CODE_v_trans_code
	ON LKP_SUP_CLAIM_TRANSACTION_CODE_v_trans_code.trans_code = v_trans_code

),
UPD_Offset_Non_Reg_Update AS (
	SELECT
	claim_trans_id, 
	trans_offset_onset_ind, 
	o_pms_trans_code AS pms_trans_code_Out, 
	o_claim_trans_code AS trans_code_out, 
	trans_amt_Out, 
	Modified_date, 
	trans_date_Out, 
	pms_acct_entered_date_Out, 
	wc_stage_pk_id_Out, 
	o_PMSTransactionCodeID, 
	o_TransactionCodeID
	FROM EXP_Offset_Update_NonReg_Res
),
claim_transaction_Update_Non_Reg_Res AS (
	MERGE INTO claim_transaction AS T
	USING UPD_Offset_Non_Reg_Update AS S
	ON T.claim_trans_id = S.claim_trans_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.pms_trans_code = S.pms_trans_code_Out, T.trans_code = S.trans_code_out, T.trans_date = S.trans_date_Out, T.pms_acct_entered_date = S.pms_acct_entered_date_Out, T.trans_amt = S.trans_amt_Out, T.wc_stage_pk_id = S.wc_stage_pk_id_Out, T.modified_date = S.Modified_date, T.trans_offset_onset_ind = S.trans_offset_onset_ind, T.PMSTransactionCodeID = S.o_PMSTransactionCodeID, T.TransactionCodeID = S.o_TransactionCodeID
),