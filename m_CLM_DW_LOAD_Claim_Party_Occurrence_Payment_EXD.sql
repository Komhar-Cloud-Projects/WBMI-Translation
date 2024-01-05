WITH
SQ_CLAIM_DRAFT_STAGE AS (
	SELECT 
	LTRIM(RTRIM(CLAIM_DRAFT_STAGE.DFT_DRAFT_NBR)),
	LTRIM(RTRIM(CLAIM_DRAFT_CLIENT_STAGE.CDC_CLAIM_NBR)), 
	LTRIM(RTRIM(CLAIM_DRAFT_CLIENT_STAGE.CDC_SEQ_NBR)), 
	LTRIM(RTRIM(CLAIM_DRAFT_CLIENT_STAGE.CDC_NAME_TYPE_IND)),
	LTRIM(RTRIM(CLAIM_DRAFT_CLIENT_STAGE.CDC_PAYEE_NM_ID))
	FROM
	 @{pipeline().parameters.SOURCE_TABLE_OWNER}.CLAIM_DRAFT_STAGE, @{pipeline().parameters.SOURCE_TABLE_OWNER}.CLAIM_DRAFT_CLIENT_STAGE
	WHERE 
	LTRIM(RTRIM(CLAIM_DRAFT_STAGE.DFT_CLAIM_NBR)) =  LTRIM(RTRIM(CLAIM_DRAFT_CLIENT_STAGE.CDC_CLAIM_NBR)) AND
	LTRIM(RTRIM(CLAIM_DRAFT_STAGE.DFT_DRAFT_NBR)) =  LTRIM(RTRIM(CLAIM_DRAFT_CLIENT_STAGE.CDC_DRAFT_NBR))
),
EXPTRANS AS (
	SELECT
	DFT_DRAFT_NBR,
	CDC_NAME_TYPE_IND,
	CDC_PAYEE_NM_ID,
	CDC_SEQ_NBR,
	CDC_CLAIM_NBR,
	1 AS crrnt_snpsht_flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	-- *INF*: TO_DATE('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS') AS eff_from_date,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS eff_to_date,
	'EXCEED' AS source_sys_id,
	SYSDATE AS created_date
	FROM SQ_CLAIM_DRAFT_STAGE
),
LKP_CLAIM_PAY_AK_ID AS (
	SELECT
	claim_pay_ak_id,
	claim_pay_num,
	IN_claim_pay_num
	FROM (
		SELECT 
		claim_payment.claim_pay_ak_id as claim_pay_ak_id, 
		RTRIM(claim_payment.claim_pay_num) as claim_pay_num 
		FROM 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.claim_payment claim_payment
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_pay_num ORDER BY claim_pay_ak_id) = 1
),
LKP_CLAIM_TRANSACTION AS (
	SELECT
	claimant_cov_det_ak_id,
	claim_pay_ak_id,
	IN_claim_pay_ak_id
	FROM (
		SELECT claim_transaction.claimant_cov_det_ak_id as claimant_cov_det_ak_id, claim_transaction.claim_pay_ak_id as claim_pay_ak_id FROM claim_transaction
		where source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_pay_ak_id ORDER BY claimant_cov_det_ak_id) = 1
),
LKP_CLAIMANT_COV_DETAIL AS (
	SELECT
	claimant_cov_det_ak_id,
	claim_party_occurrence_ak_id,
	IN_claimant_cov_det_ak_id
	FROM (
		SELECT claimant_coverage_detail.claim_party_occurrence_ak_id as claim_party_occurrence_ak_id, claimant_coverage_detail.claimant_cov_det_ak_id as claimant_cov_det_ak_id FROM claimant_coverage_detail
		WHERE source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claimant_cov_det_ak_id ORDER BY claimant_cov_det_ak_id) = 1
),
LKP_CLAIM_PARTY_OCC_AK_ID AS (
	SELECT
	claim_party_key,
	in_claim_party_key,
	claim_occurrence_key,
	in_claim_occurrence_key,
	claim_party_occurrence_ak_id
	FROM (
		SELECT 
		claim_party_occurrence.claim_party_occurrence_ak_id as claim_party_occurrence_ak_id, claim_party.claim_party_key as claim_party_key, 
		claim_occurrence.claim_occurrence_key as claim_occurrence_key 
		FROM 
		@{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_party claim_party,
		@{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_occurrence claim_occurrence, 
		@{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_party_occurrence claim_party_occurrence
		WHERE
		claim_occurrence.CLAIM_OCCURRENCE_AK_ID = claim_party_occurrence.CLAIM_OCCURRENCE_AK_ID AND
		claim_party_occurrence.CLAIM_PARTY_AK_ID = claim_party.CLAIM_PARTY_AK_ID AND
		claim_occurrence.crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_party_key,claim_occurrence_key ORDER BY claim_party_key) = 1
),
EXP_get_CPO_ID AS (
	SELECT
	LKP_CLAIM_PARTY_OCC_AK_ID.claim_party_occurrence_ak_id AS claim_party_occurrence_ak_id_cpo,
	LKP_CLAIMANT_COV_DETAIL.claim_party_occurrence_ak_id AS claim_party_occurrence_ak_id_ccd,
	-- *INF*: IIF(ISNULL(claim_party_occurrence_ak_id_cpo), claim_party_occurrence_ak_id_ccd, claim_party_occurrence_ak_id_cpo)
	IFF(claim_party_occurrence_ak_id_cpo IS NULL, claim_party_occurrence_ak_id_ccd, claim_party_occurrence_ak_id_cpo) AS CLAIM_PARTY_OCCURRENCE_AK_ID_OUT
	FROM 
	LEFT JOIN LKP_CLAIMANT_COV_DETAIL
	ON LKP_CLAIMANT_COV_DETAIL.claimant_cov_det_ak_id = LKP_CLAIM_TRANSACTION.claimant_cov_det_ak_id
	LEFT JOIN LKP_CLAIM_PARTY_OCC_AK_ID
	ON LKP_CLAIM_PARTY_OCC_AK_ID.claim_party_key = EXPTRANS.CDC_PAYEE_NM_ID AND LKP_CLAIM_PARTY_OCC_AK_ID.claim_occurrence_key = EXPTRANS.CDC_CLAIM_NBR
),
LKP_CLAIM_PARTY_OCCURRENCE_PAYMENT AS (
	SELECT
	claim_party_occurrence_pay_id,
	claim_pay_ak_id,
	claim_party_occurrence_ak_id,
	payee_code
	FROM (
		SELECT 
		claim_party_occurrence_payment.claim_party_occurrence_pay_id as claim_party_occurrence_pay_id,
		LTRIM(RTRIM(claim_party_occurrence_payment.payee_code)) as payee_code, 
		claim_party_occurrence_payment.claim_pay_ak_id as claim_pay_ak_id, 
		claim_party_occurrence_payment.claim_party_occurrence_ak_id as claim_party_occurrence_ak_id 
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_party_occurrence_payment
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_pay_ak_id,claim_party_occurrence_ak_id,payee_code ORDER BY claim_party_occurrence_pay_id) = 1
),
RTRTRANS AS (
	SELECT
	LKP_CLAIM_PAY_AK_ID.claim_pay_ak_id,
	EXP_get_CPO_ID.CLAIM_PARTY_OCCURRENCE_AK_ID_OUT AS claim_party_occurrence_ak_id,
	EXPTRANS.CDC_NAME_TYPE_IND,
	EXPTRANS.CDC_PAYEE_NM_ID,
	EXPTRANS.CDC_SEQ_NBR,
	EXPTRANS.crrnt_snpsht_flag,
	EXPTRANS.audit_id,
	EXPTRANS.eff_from_date,
	EXPTRANS.eff_to_date,
	EXPTRANS.source_sys_id,
	EXPTRANS.created_date,
	LKP_CLAIM_PARTY_OCCURRENCE_PAYMENT.claim_party_occurrence_pay_id AS exists_claim_party_occurrence_pay_id
	FROM EXPTRANS
	 -- Manually join with EXP_get_CPO_ID
	LEFT JOIN LKP_CLAIM_PARTY_OCCURRENCE_PAYMENT
	ON LKP_CLAIM_PARTY_OCCURRENCE_PAYMENT.claim_pay_ak_id = LKP_CLAIM_PAY_AK_ID.claim_pay_ak_id AND LKP_CLAIM_PARTY_OCCURRENCE_PAYMENT.claim_party_occurrence_ak_id = EXP_get_CPO_ID.CLAIM_PARTY_OCCURRENCE_AK_ID_OUT AND LKP_CLAIM_PARTY_OCCURRENCE_PAYMENT.payee_code = EXPTRANS.CDC_PAYEE_NM_ID
	LEFT JOIN LKP_CLAIM_PAY_AK_ID
	ON LKP_CLAIM_PAY_AK_ID.claim_pay_num = EXPTRANS.DFT_DRAFT_NBR
),
RTRTRANS_INSERT AS (SELECT * FROM RTRTRANS WHERE ISNULL(exists_claim_party_occurrence_pay_id)),
RTRTRANS_DEFAULT1 AS (SELECT * FROM RTRTRANS WHERE NOT ( (ISNULL(exists_claim_party_occurrence_pay_id)) )),
UPD_CLAIM_PARTY_OCC_PMT AS (
	SELECT
	exists_claim_party_occurrence_pay_id AS exists_claim_party_occurrence_pay_id2, 
	claim_pay_ak_id AS claim_pay_ak_id2, 
	claim_party_occurrence_ak_id AS claim_party_occurrence_ak_id2, 
	CDC_NAME_TYPE_IND AS CDC_NAME_TYPE_IND2, 
	CDC_PAYEE_NM_ID AS CDC_PAYEE_NM_ID2, 
	CDC_SEQ_NBR AS CDC_SEQ_NBR2, 
	audit_id AS audit_id2, 
	created_date AS created_date2
	FROM RTRTRANS_DEFAULT1
),
claim_party_occurrence_payment_update AS (
	MERGE INTO claim_party_occurrence_payment AS T
	USING UPD_CLAIM_PARTY_OCC_PMT AS S
	ON T.claim_party_occurrence_pay_id = S.exists_claim_party_occurrence_pay_id2
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.claim_pay_ak_id = S.claim_pay_ak_id2, T.claim_party_occurrence_ak_id = S.claim_party_occurrence_ak_id2, T.payee_type = S.CDC_NAME_TYPE_IND2, T.payee_code = S.CDC_PAYEE_NM_ID2, T.claim_payee_seq_num = S.CDC_SEQ_NBR2, T.audit_id = S.audit_id2, T.modified_date = S.created_date2
),
SEQ_Claim_Party_Occurrence_Payment_AK AS (
	CREATE SEQUENCE SEQ_Claim_Party_Occurrence_Payment_AK
	START = 0
	INCREMENT = 1;
),
claim_party_occurrence_payment_insert AS (
	INSERT INTO claim_party_occurrence_payment
	(claim_party_occurrence_pay_ak_id, claim_pay_ak_id, claim_party_occurrence_ak_id, payee_type, payee_code, claim_payee_seq_num, crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date)
	SELECT 
	SEQ_Claim_Party_Occurrence_Payment_AK.NEXTVAL AS CLAIM_PARTY_OCCURRENCE_PAY_AK_ID, 
	CLAIM_PAY_AK_ID, 
	CLAIM_PARTY_OCCURRENCE_AK_ID, 
	CDC_NAME_TYPE_IND AS PAYEE_TYPE, 
	CDC_PAYEE_NM_ID AS PAYEE_CODE, 
	CDC_SEQ_NBR AS CLAIM_PAYEE_SEQ_NUM, 
	CRRNT_SNPSHT_FLAG, 
	AUDIT_ID, 
	EFF_FROM_DATE, 
	EFF_TO_DATE, 
	SOURCE_SYS_ID, 
	CREATED_DATE, 
	created_date AS MODIFIED_DATE
	FROM RTRTRANS_INSERT
),