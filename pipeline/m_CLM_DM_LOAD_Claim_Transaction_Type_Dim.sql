WITH
LKP_sup_claim_transaction_code AS (
	SELECT
	trans_descript,
	trans_code
	FROM (
		SELECT sup_claim_transaction_code.trans_descript AS trans_descript,
		       sup_claim_transaction_code.trans_code     AS trans_code
		FROM   @{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_claim_transaction_code
		WHERE  sup_claim_transaction_code.crrnt_snpsht_flag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY trans_code ORDER BY trans_descript DESC) = 1
),
SQ_claim_reinsurance_transaction AS (
	SELECT DISTINCT claim_reinsurance_transaction.type_disability                 ,
	       claim_reinsurance_transaction.claim_reins_trans_code          ,
	       'N/A' as s3p_trans_code                 ,
	       claim_reinsurance_transaction.claim_reins_pms_trans_code      ,
	       claim_reinsurance_transaction.claim_reins_trans_base_type_code,
	       claim_reinsurance_transaction.trans_ctgry_code                ,
	       'N/A' AS trans_rsn,
	       'C'   AS trans_kind_code,
	       claim_reinsurance_transaction.offset_onset_ind
	FROM   claim_reinsurance_transaction
	WHERE  claim_reinsurance_transaction.modified_date >= '@{pipeline().parameters.SELECTION_START_TS}'
),
SQ_claim_transaction AS (
	SELECT DISTINCT claim_transaction.type_disability     ,
	                claim_transaction.offset_onset_ind    ,
	                claim_transaction.s3p_trans_code      ,
	                claim_transaction.pms_trans_code      ,
	                claim_transaction.trans_code          ,
	                claim_transaction.trans_base_type_code,
	                claim_transaction.trans_ctgry_code    ,
	                claim_transaction.trans_rsn           ,
	                'D' AS trans_kind_code
	FROM            @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_transaction
	WHERE           claim_transaction.modified_date >='@{pipeline().parameters.SELECTION_START_TS}'
	
	UNION
	
	SELECT DISTINCT claim_transaction.type_disability     ,
	                claim_transaction.offset_onset_ind    ,
	                claim_transaction.s3p_trans_code      ,
	                claim_transaction.pms_trans_code      ,
	                claim_transaction.trans_code          ,
	                claim_transaction.trans_base_type_code,
	                claim_transaction.trans_ctgry_code    ,
	                claim_transaction.trans_rsn           ,
	                'C' AS trans_kind_code
	FROM            @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_transaction
	WHERE           claim_transaction.modified_date >='@{pipeline().parameters.SELECTION_START_TS}'
),
Union AS (
	SELECT type_disability, trans_code, s3p_trans_code, pms_trans_code, trans_base_type_code, trans_ctgry_code, trans_rsn, trans_kind_code, offset_onset_ind
	FROM SQ_claim_transaction
	UNION
	SELECT type_disability, claim_reins_trans_code AS trans_code, s3p_trans_code, claim_reins_pms_trans_code AS pms_trans_code, claim_reins_trans_base_type_code AS trans_base_type_code, trans_ctgry_code, trans_rsn, trans_kind_code, offset_onset_ind
	FROM SQ_claim_reinsurance_transaction
),
SRT_Remove_Duplicate_Rows AS (
	SELECT
	type_disability, 
	trans_code, 
	s3p_trans_code, 
	pms_trans_code, 
	trans_base_type_code, 
	trans_ctgry_code, 
	trans_rsn, 
	trans_kind_code, 
	offset_onset_ind
	FROM Union
	ORDER BY type_disability ASC, trans_code ASC, s3p_trans_code ASC, pms_trans_code ASC, trans_base_type_code ASC, trans_ctgry_code ASC, trans_rsn ASC, trans_kind_code ASC, offset_onset_ind ASC
),
LKP_Claim_transaction_type_dim AS (
	SELECT
	claim_trans_type_dim_id,
	trans_ctgry_code,
	trans_code,
	s3p_trans_code,
	pms_trans_code,
	trans_base_type_code,
	trans_rsn,
	type_disability,
	trans_kind_code,
	offset_onset_ind
	FROM (
		SELECT 
			claim_trans_type_dim_id,
			trans_ctgry_code,
			trans_code,
			s3p_trans_code,
			pms_trans_code,
			trans_base_type_code,
			trans_rsn,
			type_disability,
			trans_kind_code,
			offset_onset_ind
		FROM claim_transaction_type_dim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY trans_ctgry_code,trans_code,s3p_trans_code,pms_trans_code,trans_base_type_code,trans_rsn,type_disability,trans_kind_code,offset_onset_ind ORDER BY claim_trans_type_dim_id DESC) = 1
),
LKP_sup_claim_transaction_category AS (
	SELECT
	trans_ctgry_descript,
	trans_ctgry_code
	FROM (
		SELECT sup_claim_transaction_category.trans_ctgry_descript AS trans_ctgry_descript,
		       sup_claim_transaction_category.trans_ctgry_code     AS trans_ctgry_code
		FROM   @{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_claim_transaction_category
		WHERE  sup_claim_transaction_category.crrnt_snpsht_flag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY trans_ctgry_code ORDER BY trans_ctgry_descript DESC) = 1
),
LKP_sup_claim_type_disability AS (
	SELECT
	type_disability_descript,
	type_disability
	FROM (
		SELECT sup_claim_type_disability.type_disability_descript AS type_disability_descript,
		       sup_claim_type_disability.type_disability          AS type_disability
		FROM   @{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_claim_type_disability
		WHERE  sup_claim_type_disability.crrnt_snpsht_flag =1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY type_disability ORDER BY type_disability_descript DESC) = 1
),
LKP_sup_transaction_reason AS (
	SELECT
	trans_rsn_descript,
	trans_rsn_code
	FROM (
		SELECT sup_claim_transaction_reason.trans_rsn_descript AS trans_rsn_descript,
		       sup_claim_transaction_reason.trans_rsn_code     AS trans_rsn_code
		FROM   @{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_claim_transaction_reason
		WHERE  sup_claim_transaction_reason.crrnt_snpsht_flag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY trans_rsn_code ORDER BY trans_rsn_descript DESC) = 1
),
EXP_get_values AS (
	SELECT
	LKP_Claim_transaction_type_dim.claim_trans_type_dim_id,
	SRT_Remove_Duplicate_Rows.trans_ctgry_code AS IN_trans_ctgry_code,
	LKP_sup_claim_transaction_category.trans_ctgry_descript AS lkp_trans_ctgry_descript_IN,
	-- *INF*: IIF(ISNULL(lkp_trans_ctgry_descript_IN), 'N/A', lkp_trans_ctgry_descript_IN)
	IFF(lkp_trans_ctgry_descript_IN IS NULL, 'N/A', lkp_trans_ctgry_descript_IN) AS lkp_trans_ctgry_descript_OUT,
	SRT_Remove_Duplicate_Rows.trans_code AS IN_trans_code,
	-- *INF*: :LKP.LKP_SUP_CLAIM_TRANSACTION_CODE(IN_trans_code)
	LKP_SUP_CLAIM_TRANSACTION_CODE_IN_trans_code.trans_descript AS lkp_trans_descript_IN,
	-- *INF*: IIF(ISNULL(lkp_trans_descript_IN), 'N/A', lkp_trans_descript_IN)
	IFF(lkp_trans_descript_IN IS NULL, 'N/A', lkp_trans_descript_IN) AS lkp_trans_code_descript_OUT,
	SRT_Remove_Duplicate_Rows.trans_base_type_code AS IN_trans_base_type_code,
	-- *INF*: :LKP.LKP_SUP_CLAIM_TRANSACTION_CODE(IN_trans_base_type_code)
	LKP_SUP_CLAIM_TRANSACTION_CODE_IN_trans_base_type_code.trans_descript AS lkp_trans_base_type_code_descript_IN,
	-- *INF*: IIF(ISNULL(lkp_trans_base_type_code_descript_IN), 'N/A', lkp_trans_base_type_code_descript_IN)
	IFF(lkp_trans_base_type_code_descript_IN IS NULL, 'N/A', lkp_trans_base_type_code_descript_IN) AS lkp_trans_base_type_code_descript_OUT,
	SRT_Remove_Duplicate_Rows.trans_rsn AS IN_trans_rsn,
	LKP_sup_transaction_reason.trans_rsn_descript AS lkp_trans_rsn_descript_IN,
	-- *INF*: IIF(ISNULL(lkp_trans_rsn_descript_IN), 'N/A', lkp_trans_rsn_descript_IN)
	IFF(lkp_trans_rsn_descript_IN IS NULL, 'N/A', lkp_trans_rsn_descript_IN) AS lkp_trans_rsn_descript_OUT,
	SRT_Remove_Duplicate_Rows.type_disability AS IN_type_disability_code,
	LKP_sup_claim_type_disability.type_disability_descript AS lkp_type_disability_descript_IN,
	-- *INF*: iif(isnull(lkp_type_disability_descript_IN),'N/A',lkp_type_disability_descript_IN)
	IFF(lkp_type_disability_descript_IN IS NULL, 'N/A', lkp_type_disability_descript_IN) AS lkp_type_disability_descript_OUT,
	1 AS crrnt_snpsht_flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	-- *INF*: TO_DATE('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS')
	TO_DATE('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS') AS eff_from_date,
	-- *INF*: TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS eff_to_date,
	SYSDATE AS created_date,
	SYSDATE AS modified_date,
	SRT_Remove_Duplicate_Rows.trans_kind_code,
	-- *INF*: DECODE(trans_kind_code,
	-- 	'D', 'Direct',
	-- 	'C', 'Ceded')
	-- 
	DECODE(trans_kind_code,
	'D', 'Direct',
	'C', 'Ceded') AS trans_kind_desc,
	SRT_Remove_Duplicate_Rows.offset_onset_ind,
	SRT_Remove_Duplicate_Rows.s3p_trans_code,
	SRT_Remove_Duplicate_Rows.pms_trans_code,
	-- *INF*: :LKP.LKP_SUP_CLAIM_TRANSACTION_CODE(pms_trans_code)
	LKP_SUP_CLAIM_TRANSACTION_CODE_pms_trans_code.trans_descript AS v_pms_trans_code_descript,
	-- *INF*: IIF(ISNULL(v_pms_trans_code_descript), 'N/A', v_pms_trans_code_descript)
	IFF(v_pms_trans_code_descript IS NULL, 'N/A', v_pms_trans_code_descript) AS pms_trans_code_descript,
	'N/A' AS Default_NA
	FROM SRT_Remove_Duplicate_Rows
	LEFT JOIN LKP_Claim_transaction_type_dim
	ON LKP_Claim_transaction_type_dim.trans_ctgry_code = SRT_Remove_Duplicate_Rows.trans_ctgry_code AND LKP_Claim_transaction_type_dim.trans_code = SRT_Remove_Duplicate_Rows.trans_code AND LKP_Claim_transaction_type_dim.s3p_trans_code = SRT_Remove_Duplicate_Rows.s3p_trans_code AND LKP_Claim_transaction_type_dim.pms_trans_code = SRT_Remove_Duplicate_Rows.pms_trans_code AND LKP_Claim_transaction_type_dim.trans_base_type_code = SRT_Remove_Duplicate_Rows.trans_base_type_code AND LKP_Claim_transaction_type_dim.trans_rsn = SRT_Remove_Duplicate_Rows.trans_rsn AND LKP_Claim_transaction_type_dim.type_disability = SRT_Remove_Duplicate_Rows.type_disability AND LKP_Claim_transaction_type_dim.trans_kind_code = SRT_Remove_Duplicate_Rows.trans_kind_code AND LKP_Claim_transaction_type_dim.offset_onset_ind = SRT_Remove_Duplicate_Rows.offset_onset_ind
	LEFT JOIN LKP_sup_claim_transaction_category
	ON LKP_sup_claim_transaction_category.trans_ctgry_code = SRT_Remove_Duplicate_Rows.trans_ctgry_code
	LEFT JOIN LKP_sup_claim_type_disability
	ON LKP_sup_claim_type_disability.type_disability = SRT_Remove_Duplicate_Rows.type_disability
	LEFT JOIN LKP_sup_transaction_reason
	ON LKP_sup_transaction_reason.trans_rsn_code = SRT_Remove_Duplicate_Rows.trans_rsn
	LEFT JOIN LKP_SUP_CLAIM_TRANSACTION_CODE LKP_SUP_CLAIM_TRANSACTION_CODE_IN_trans_code
	ON LKP_SUP_CLAIM_TRANSACTION_CODE_IN_trans_code.trans_code = IN_trans_code

	LEFT JOIN LKP_SUP_CLAIM_TRANSACTION_CODE LKP_SUP_CLAIM_TRANSACTION_CODE_IN_trans_base_type_code
	ON LKP_SUP_CLAIM_TRANSACTION_CODE_IN_trans_base_type_code.trans_code = IN_trans_base_type_code

	LEFT JOIN LKP_SUP_CLAIM_TRANSACTION_CODE LKP_SUP_CLAIM_TRANSACTION_CODE_pms_trans_code
	ON LKP_SUP_CLAIM_TRANSACTION_CODE_pms_trans_code.trans_code = pms_trans_code

),
FIL_Claim_Transaction_Type_Dim AS (
	SELECT
	claim_trans_type_dim_id, 
	crrnt_snpsht_flag, 
	audit_id, 
	eff_from_date, 
	eff_to_date, 
	created_date, 
	modified_date, 
	IN_trans_ctgry_code AS trans_ctgry_code_s, 
	lkp_trans_ctgry_descript_OUT, 
	IN_trans_code AS trans_code_s, 
	lkp_trans_code_descript_OUT, 
	IN_trans_base_type_code AS trans_base_type_code_s, 
	lkp_trans_base_type_code_descript_OUT AS lkp_trans_base_type_code_descript_out, 
	trans_kind_code, 
	trans_kind_desc, 
	IN_trans_rsn AS trans_rsn_s, 
	lkp_trans_rsn_descript_OUT AS lkp_trans_rsn_descript_out, 
	IN_type_disability_code AS type_disability_code, 
	lkp_type_disability_descript_OUT AS lkp_type_disability_descript_out, 
	offset_onset_ind, 
	s3p_trans_code, 
	Default_NA AS s3p_trans_code_descript, 
	pms_trans_code, 
	pms_trans_code_descript
	FROM EXP_get_values
	WHERE IIF(ISNULL(claim_trans_type_dim_id),TRUE,FALSE)
),
EXP_Default AS (
	SELECT
	crrnt_snpsht_flag,
	audit_id,
	eff_from_date,
	eff_to_date,
	created_date,
	modified_date,
	trans_ctgry_code_s,
	lkp_trans_ctgry_descript_OUT,
	trans_code_s,
	lkp_trans_code_descript_OUT AS lkp_trans_code_descript_out,
	s3p_trans_code,
	s3p_trans_code_descript,
	pms_trans_code,
	pms_trans_code_descript,
	trans_base_type_code_s,
	lkp_trans_base_type_code_descript_out,
	trans_kind_code,
	trans_kind_desc,
	trans_rsn_s,
	lkp_trans_rsn_descript_out,
	type_disability_code,
	lkp_type_disability_descript_out,
	offset_onset_ind
	FROM FIL_Claim_Transaction_Type_Dim
),
claim_transaction_type_dim_insert AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_transaction_type_dim
	(crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, created_date, modified_date, trans_ctgry_code, trans_ctgry_code_descript, trans_code, trans_code_descript, s3p_trans_code, s3p_trans_code_descript, pms_trans_code, pms_trans_code_descript, trans_base_type_code, trans_base_type_code_descript, trans_kind_code, trans_kind_code_descript, trans_rsn, trans_rsn_descript, type_disability, type_disability_descript, offset_onset_ind)
	SELECT 
	CRRNT_SNPSHT_FLAG, 
	AUDIT_ID, 
	EFF_FROM_DATE, 
	EFF_TO_DATE, 
	CREATED_DATE, 
	MODIFIED_DATE, 
	trans_ctgry_code_s AS TRANS_CTGRY_CODE, 
	lkp_trans_ctgry_descript_OUT AS TRANS_CTGRY_CODE_DESCRIPT, 
	trans_code_s AS TRANS_CODE, 
	lkp_trans_code_descript_out AS TRANS_CODE_DESCRIPT, 
	S3P_TRANS_CODE, 
	S3P_TRANS_CODE_DESCRIPT, 
	PMS_TRANS_CODE, 
	PMS_TRANS_CODE_DESCRIPT, 
	trans_base_type_code_s AS TRANS_BASE_TYPE_CODE, 
	lkp_trans_base_type_code_descript_out AS TRANS_BASE_TYPE_CODE_DESCRIPT, 
	TRANS_KIND_CODE, 
	trans_kind_desc AS TRANS_KIND_CODE_DESCRIPT, 
	trans_rsn_s AS TRANS_RSN, 
	lkp_trans_rsn_descript_out AS TRANS_RSN_DESCRIPT, 
	type_disability_code AS TYPE_DISABILITY, 
	lkp_type_disability_descript_out AS TYPE_DISABILITY_DESCRIPT, 
	OFFSET_ONSET_IND
	FROM EXP_Default
),