WITH
SQ_claim_transaction AS (
	SELECT 
	claim_transaction.claim_trans_id, 
	claim_transaction.tax_id
	FROM
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_transaction
	where claim_transaction.tax_id <>'000000000'
	and claim_master_1099_list_ak_id =-1
),
EXP_claim_master_1099_list_ak_id AS (
	SELECT
	claim_trans_id,
	tax_id AS in_tax_id,
	-- *INF*: ltrim(rtrim(in_tax_id))
	ltrim(rtrim(in_tax_id
		)
	) AS tax_id
	FROM SQ_claim_transaction
),
LKP_claim_master_1099_list AS (
	SELECT
	claim_master_1099_list_ak_id,
	tax_id
	FROM (
		SELECT
		 claim_master_1099_list.claim_master_1099_list_ak_id as claim_master_1099_list_ak_id, LTRIM(RTRIM(claim_master_1099_list.tax_id)) as tax_id 
		FROM 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.claim_master_1099_list
		where 
		claim_master_1099_list.crrnt_snpsht_flag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY tax_id ORDER BY claim_master_1099_list_ak_id) = 1
),
EXP_claim_master_ak_id AS (
	SELECT
	EXP_claim_master_1099_list_ak_id.claim_trans_id,
	LKP_claim_master_1099_list.claim_master_1099_list_ak_id AS lkp_claim_master_1099_list_ak_id,
	-- *INF*: iif(isnull(lkp_claim_master_1099_list_ak_id),-1,lkp_claim_master_1099_list_ak_id)
	IFF(lkp_claim_master_1099_list_ak_id IS NULL,
		- 1,
		lkp_claim_master_1099_list_ak_id
	) AS claim_master_1099_list_ak_id
	FROM EXP_claim_master_1099_list_ak_id
	LEFT JOIN LKP_claim_master_1099_list
	ON LKP_claim_master_1099_list.tax_id = EXP_claim_master_1099_list_ak_id.tax_id
),
UPD_master_1099_ak_id AS (
	SELECT
	claim_trans_id, 
	claim_master_1099_list_ak_id
	FROM EXP_claim_master_ak_id
),
TGT_claim_transaction_UPDATE AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_transaction AS T
	USING UPD_master_1099_ak_id AS S
	ON T.claim_trans_id = S.claim_trans_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.claim_master_1099_list_ak_id = S.claim_master_1099_list_ak_id
),