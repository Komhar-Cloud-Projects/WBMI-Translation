WITH
LKP_Clm_Offset_Honor_Stage AS (
	SELECT
	COH_DRAFT_NBR
	FROM (
		SELECT 
			COH_DRAFT_NBR
		FROM CLM_OFFSET_HONOR_STAGE
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY COH_DRAFT_NBR ORDER BY COH_DRAFT_NBR) = 1
),
LKP_Claim_Support_01_Exceed_PMS_Trans_Code AS (
	SELECT
	cs01_code_des,
	cs01_code
	FROM (
		SELECT 
		LTRIM(RTRIM(A.cs01_code_des)) as cs01_code_des, 
		LTRIM(RTRIM(A.cs01_code)) as cs01_code FROM claim_support_01_stage A
		WHERE A.cs01_table_id = 'R003'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY cs01_code ORDER BY cs01_code_des DESC) = 1
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
SQ_CLAIM_TRANSACTION_STAGE_INS AS (
	SELECT A.CLAIM_TRANSACTION_ID,
	       A.CTX_CLAIM_NBR,
	       A.CTX_CLIENT_ID,
	       A.CTX_OBJECT_TYPE_CD,
	       A.CTX_OBJECT_SEQ_NBR,
	       A.CTX_COV_TYPE_CD,
	       A.CTX_COV_SEQ_NBR,
	       A.CTX_BUR_CAUSE_LOSS,
	       A.CTX_FIN_TYPE_CD,
	       A.CTX_SORT_TS,
	       A.CTX_TRS_TYPE_CD,
	       A.CTX_DRAFT_NBR,
	       A.CTX_UPD_TS,
	       A.CTX_TRS_CAT_CD,
	       A.CTX_TRS_AMT,
	       A.CTX_ENTRY_OPR_ID,
	       A.CTX_TRS_HST_AMT,
	       A.CTX_BASE_TRS_TYPE,
	       A.CTX_OFS_REI_IND,
	       A.CTX_SINGLE_CHK_IND,
	       A.CTX_CREATE_TS,
	       A.CTX_TRANS_REASON
	FROM   @{pipeline().parameters.SOURCE_TABLE_OWNER}.CLAIM_TRANSACTION_STAGE A
),
EXP_Default1 AS (
	SELECT
	CLAIM_TRANSACTION_ID,
	CTX_CLAIM_NBR,
	CTX_CLIENT_ID,
	CTX_OBJECT_TYPE_CD,
	CTX_OBJECT_SEQ_NBR,
	CTX_COV_TYPE_CD,
	CTX_COV_SEQ_NBR,
	CTX_BUR_CAUSE_LOSS,
	CTX_FIN_TYPE_CD,
	CTX_SORT_TS,
	CTX_TRS_TYPE_CD,
	CTX_DRAFT_NBR,
	CTX_UPD_TS,
	CTX_TRS_CAT_CD,
	CTX_TRS_AMT,
	CTX_ENTRY_OPR_ID,
	CTX_TRS_HST_AMT,
	CTX_BASE_TRS_TYPE,
	CTX_OFS_REI_IND,
	CTX_SINGLE_CHK_IND,
	CTX_CREATE_TS,
	CTX_TRANS_REASON
	FROM SQ_CLAIM_TRANSACTION_STAGE_INS
),
EXP_Default AS (
	SELECT
	CTX_FIN_TYPE_CD,
	CTX_TRS_TYPE_CD,
	-- *INF*: TO_CHAR(CTX_TRS_TYPE_CD)
	TO_CHAR(CTX_TRS_TYPE_CD) AS out_CTX_TRS_TYPE_CD,
	CTX_TRS_CAT_CD,
	-- *INF*: IIF((ISNULL(CTX_TRS_CAT_CD) OR IS_SPACES(CTX_TRS_CAT_CD)), 'N/A',ltrim(rtrim(CTX_TRS_CAT_CD)))
	IFF(
	    (CTX_TRS_CAT_CD IS NULL OR LENGTH(CTX_TRS_CAT_CD)>0 AND TRIM(CTX_TRS_CAT_CD)=''), 'N/A',
	    ltrim(rtrim(CTX_TRS_CAT_CD))
	) AS OP_CTX_TRS_CAT_CD,
	CTX_CREATE_TS AS IN_CTX_CREATE_TS,
	-- *INF*: IIF(ISNULL(IN_CTX_CREATE_TS),TO_DATE('1/1/1800','MM/DD/YYYY'),IN_CTX_CREATE_TS)
	IFF(IN_CTX_CREATE_TS IS NULL, TO_TIMESTAMP('1/1/1800', 'MM/DD/YYYY'), IN_CTX_CREATE_TS) AS CTX_CREATE_TS
	FROM EXP_Default1
),
LKP_Sup_Convert_S3p_Claim_Transaction_Code AS (
	SELECT
	edw_financial_type_code,
	edw_trans_code,
	edw_trans_ctgry_code,
	s3p_financial_type_code,
	s3p_trans_code,
	s3p_trans_ctgry_code
	FROM (
		SELECT sup_convert_s3p_claim_transaction_code.edw_financial_type_code as edw_financial_type_code, sup_convert_s3p_claim_transaction_code.edw_trans_code as edw_trans_code, sup_convert_s3p_claim_transaction_code.edw_trans_ctgry_code as edw_trans_ctgry_code, sup_convert_s3p_claim_transaction_code.s3p_financial_type_code as s3p_financial_type_code, sup_convert_s3p_claim_transaction_code.s3p_trans_code as s3p_trans_code, sup_convert_s3p_claim_transaction_code.s3p_trans_ctgry_code as s3p_trans_ctgry_code 
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_convert_s3p_claim_transaction_code
		WHERE crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY s3p_financial_type_code,s3p_trans_code,s3p_trans_ctgry_code ORDER BY edw_financial_type_code) = 1
),
EXP_Chk_Null_Lkp AS (
	SELECT
	LKP_Sup_Convert_S3p_Claim_Transaction_Code.edw_financial_type_code,
	LKP_Sup_Convert_S3p_Claim_Transaction_Code.edw_trans_code,
	LKP_Sup_Convert_S3p_Claim_Transaction_Code.edw_trans_ctgry_code,
	EXP_Default.CTX_FIN_TYPE_CD,
	EXP_Default.out_CTX_TRS_TYPE_CD,
	EXP_Default.OP_CTX_TRS_CAT_CD,
	-- *INF*: IIF(ISNULL(edw_financial_type_code),CTX_FIN_TYPE_CD,edw_financial_type_code)
	IFF(edw_financial_type_code IS NULL, CTX_FIN_TYPE_CD, edw_financial_type_code) AS op_edw_financial_type_code,
	-- *INF*: IIF(ISNULL(edw_trans_code),out_CTX_TRS_TYPE_CD,edw_trans_code)
	IFF(edw_trans_code IS NULL, out_CTX_TRS_TYPE_CD, edw_trans_code) AS op_edw_trans_code,
	-- *INF*: IIF(ISNULL(edw_trans_ctgry_code),OP_CTX_TRS_CAT_CD,edw_trans_ctgry_code)
	IFF(edw_trans_ctgry_code IS NULL, OP_CTX_TRS_CAT_CD, edw_trans_ctgry_code) AS op_edw_trans_ctgry_code
	FROM EXP_Default
	LEFT JOIN LKP_Sup_Convert_S3p_Claim_Transaction_Code
	ON LKP_Sup_Convert_S3p_Claim_Transaction_Code.s3p_financial_type_code = EXP_Default.CTX_FIN_TYPE_CD AND LKP_Sup_Convert_S3p_Claim_Transaction_Code.s3p_trans_code = EXP_Default.out_CTX_TRS_TYPE_CD AND LKP_Sup_Convert_S3p_Claim_Transaction_Code.s3p_trans_ctgry_code = EXP_Default.OP_CTX_TRS_CAT_CD
),
LKP_Claim_Draft_Stage AS (
	SELECT
	DFT_TAX_ID_NBR,
	DFT_TAX_ID_TYPE_CD,
	DFT_CLAIM_NBR,
	DFT_DRAFT_NBR
	FROM (
		SELECT 
		CLAIM_DRAFT_STAGE.DFT_TAX_ID_NBR as DFT_TAX_ID_NBR, CLAIM_DRAFT_STAGE.DFT_TAX_ID_TYPE_CD as DFT_TAX_ID_TYPE_CD, CLAIM_DRAFT_STAGE.DFT_CLAIM_NBR as DFT_CLAIM_NBR, 
		CLAIM_DRAFT_STAGE.DFT_DRAFT_NBR as DFT_DRAFT_NBR 
		FROM 
		CLAIM_DRAFT_STAGE
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY DFT_CLAIM_NBR,DFT_DRAFT_NBR ORDER BY DFT_TAX_ID_NBR) = 1
),
LKP_Claim_1099_Master_List_connected AS (
	SELECT
	claim_master_1099_list_ak_id,
	tax_id,
	irs_tax_id
	FROM (
		SELECT 
		claim_master_1099_list.claim_master_1099_list_ak_id as claim_master_1099_list_ak_id, 
		claim_master_1099_list.tax_id as tax_id, 
		claim_master_1099_list.irs_tax_id as irs_tax_id 
		FROM 
		claim_master_1099_list
		where 
		claim_master_1099_list.crrnt_snpsht_flag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY irs_tax_id ORDER BY claim_master_1099_list_ak_id) = 1
),
LKP_Claim_Party_Occurrence_AK_ID AS (
	SELECT
	claim_party_occurrence_ak_id,
	offset_onset_ind,
	pms_pol_lob_code,
	claimant_num,
	claim_party_role_code
	FROM (
		SELECT CPO.claim_party_occurrence_ak_id as claim_party_occurrence_ak_id, 
		CO.claim_occurrence_type_code as offset_onset_ind,
		LTRIM(RTRIM(CO.claim_occurrence_key)) as claimant_num, 
		LTRIM(RTRIM(CP.claim_party_key)) as claim_party_role_code,
		LTRIM(RTRIM(VP.pms_pol_lob_code)) as pms_pol_lob_code
		FROM 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.claim_party_occurrence CPO,
		@{pipeline().parameters.TARGET_TABLE_OWNER}.claim_party CP, 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.claim_occurrence CO,
		V2.policy VP
		WHERE CO.claim_occurrence_ak_id = CPO.claim_occurrence_ak_id  AND CP.claim_party_ak_id = CPO.claim_party_ak_id AND VP.pol_ak_id=CO.pol_key_ak_id
		AND CO.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' 
		AND CP.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' 
		AND CPO.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' 
		AND CPO.claim_party_role_code = 'CLMT'
		AND CO.crrnt_snpsht_flag = 1
		AND CP.crrnt_snpsht_flag = 1
		AND CPO.crrnt_snpsht_flag = 1
		AND VP.crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claimant_num,claim_party_role_code ORDER BY claim_party_occurrence_ak_id) = 1
),
LKP_Claim_Payment AS (
	SELECT
	claim_pay_ak_id,
	claim_pay_num,
	IN_CTX_DRAFT_NBR
	FROM (
		SELECT 
			claim_pay_ak_id,
			claim_pay_num,
			IN_CTX_DRAFT_NBR
		FROM claim_payment
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_pay_num ORDER BY claim_pay_ak_id) = 1
),
LKP_Clm_Cov_Pkg_Stage AS (
	SELECT
	CCP_MNL_ENTRY_IND,
	CCP_SAR_ID,
	CCP_CLAIM_NBR,
	CCP_OBJECT_TYPE_CD,
	CCP_OBJECT_SEQ_NBR,
	CCP_PKG_TYPE_CD,
	CCP_PKG_SEQ_NBR
	FROM (
		SELECT 
			CCP_MNL_ENTRY_IND,
			CCP_SAR_ID,
			CCP_CLAIM_NBR,
			CCP_OBJECT_TYPE_CD,
			CCP_OBJECT_SEQ_NBR,
			CCP_PKG_TYPE_CD,
			CCP_PKG_SEQ_NBR
		FROM CLM_COV_PKG_STAGE
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY CCP_CLAIM_NBR,CCP_OBJECT_TYPE_CD,CCP_OBJECT_SEQ_NBR,CCP_PKG_TYPE_CD,CCP_PKG_SEQ_NBR ORDER BY CCP_MNL_ENTRY_IND DESC) = 1
),
EXP_Source AS (
	SELECT
	EXP_Default1.CTX_OBJECT_TYPE_CD,
	LKP_Claim_Party_Occurrence_AK_ID.claim_party_occurrence_ak_id,
	EXP_Default1.CTX_BUR_CAUSE_LOSS,
	-- *INF*: SUBSTR(CTX_BUR_CAUSE_LOSS,1,2)
	SUBSTR(CTX_BUR_CAUSE_LOSS, 1, 2) AS V_CAUSE_LOSS,
	V_CAUSE_LOSS AS OP_CAUSE_LOSS,
	-- *INF*: SUBSTR(CTX_BUR_CAUSE_LOSS,3,1)
	SUBSTR(CTX_BUR_CAUSE_LOSS, 3, 1) AS V_RESERVE_CAT,
	V_RESERVE_CAT AS OP_RESERVE_CAT,
	-- *INF*: IIF(CTX_OBJECT_TYPE_CD = 'WCC' AND SUBSTR(CTX_BUR_CAUSE_LOSS, 1,2) = '06','06','05')
	IFF(CTX_OBJECT_TYPE_CD = 'WCC' AND SUBSTR(CTX_BUR_CAUSE_LOSS, 1, 2) = '06', '06', '05') AS TYPE_DISABILITY_OP,
	EXP_Default1.CTX_DRAFT_NBR,
	-- *INF*: IIF(ISNULL(CTX_DRAFT_NBR) OR IS_SPACES(CTX_DRAFT_NBR) ,'N/A', CTX_DRAFT_NBR)
	IFF(
	    CTX_DRAFT_NBR IS NULL OR LENGTH(CTX_DRAFT_NBR)>0 AND TRIM(CTX_DRAFT_NBR)='', 'N/A',
	    CTX_DRAFT_NBR
	) AS OP_CTX_DRAFT_NBR,
	-- *INF*: IIF (NOT ISNULL(:LKP.LKP_CLM_OFFSET_HONOR_STAGE(CTX_DRAFT_NBR)), 'O','N/A')
	IFF(LKP_CLM_OFFSET_HONOR_STAGE_CTX_DRAFT_NBR.COH_DRAFT_NBR IS NOT NULL, 'O', 'N/A') AS OP_OFFSET_ONSET_IND,
	EXP_Chk_Null_Lkp.op_edw_financial_type_code AS CTX_FIN_TYPE_CD,
	EXP_Chk_Null_Lkp.op_edw_trans_code AS CTX_TRS_TYPE_CD,
	-- *INF*: RTRIM(LTRIM(TO_CHAR(CTX_TRS_TYPE_CD)))
	RTRIM(LTRIM(TO_CHAR(CTX_TRS_TYPE_CD))) AS out_CTX_TRS_TYPE_CD,
	-- *INF*: IIF(CTX_TRS_TYPE_CD = 91 OR CTX_TRS_TYPE_CD = 92, 90, CTX_TRS_TYPE_CD)
	IFF(CTX_TRS_TYPE_CD = 91 OR CTX_TRS_TYPE_CD = 92, 90, CTX_TRS_TYPE_CD) AS OP_CTX_TRS_TYPE_CD,
	EXP_Default1.CTX_SORT_TS,
	EXP_Default1.CTX_UPD_TS,
	-- *INF*: IIF(ISNULL(CTX_UPD_TS), TO_DATE('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'),CTX_UPD_TS)
	IFF(
	    CTX_UPD_TS IS NULL, TO_TIMESTAMP('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'), CTX_UPD_TS
	) AS OP_CTX_UPD_TS,
	EXP_Default1.CTX_TRS_AMT,
	-- *INF*: IIF(ISNULL(CTX_TRS_AMT),0,CTX_TRS_AMT)
	IFF(CTX_TRS_AMT IS NULL, 0, CTX_TRS_AMT) AS OP_CTX_TRS_AMT,
	EXP_Default1.CTX_TRS_HST_AMT,
	-- *INF*: IIF(ISNULL(CTX_TRS_HST_AMT),0,CTX_TRS_HST_AMT)
	IFF(CTX_TRS_HST_AMT IS NULL, 0, CTX_TRS_HST_AMT) AS OP_CTX_TRS_HST_AMT,
	EXP_Default1.CTX_TRANS_REASON,
	-- *INF*: IIF(ISNULL(CTX_TRANS_REASON) OR IS_SPACES(CTX_TRANS_REASON),'N/A',CTX_TRANS_REASON)
	IFF(
	    CTX_TRANS_REASON IS NULL OR LENGTH(CTX_TRANS_REASON)>0 AND TRIM(CTX_TRANS_REASON)='', 'N/A',
	    CTX_TRANS_REASON
	) AS OP_CTX_TRANS_REASON,
	EXP_Default1.CTX_SINGLE_CHK_IND,
	-- *INF*: IIF(ISNULL(CTX_SINGLE_CHK_IND) OR IS_SPACES(CTX_SINGLE_CHK_IND),'N/A',CTX_SINGLE_CHK_IND)
	IFF(
	    CTX_SINGLE_CHK_IND IS NULL OR LENGTH(CTX_SINGLE_CHK_IND)>0 AND TRIM(CTX_SINGLE_CHK_IND)='',
	    'N/A',
	    CTX_SINGLE_CHK_IND
	) AS OP_CTX_SINGLE_CHK_IND,
	EXP_Default1.CTX_OFS_REI_IND,
	-- *INF*: IIF(ISNULL(CTX_OFS_REI_IND) OR IS_SPACES(CTX_OFS_REI_IND),'N/A',CTX_OFS_REI_IND)
	IFF(
	    CTX_OFS_REI_IND IS NULL OR LENGTH(CTX_OFS_REI_IND)>0 AND TRIM(CTX_OFS_REI_IND)='', 'N/A',
	    CTX_OFS_REI_IND
	) AS OP_CTX_OFS_REI_IND,
	EXP_Default1.CTX_ENTRY_OPR_ID,
	-- *INF*: IIF(ISNULL(CTX_ENTRY_OPR_ID) OR IS_SPACES(CTX_ENTRY_OPR_ID),'N/A',CTX_ENTRY_OPR_ID)
	IFF(
	    CTX_ENTRY_OPR_ID IS NULL OR LENGTH(CTX_ENTRY_OPR_ID)>0 AND TRIM(CTX_ENTRY_OPR_ID)='', 'N/A',
	    CTX_ENTRY_OPR_ID
	) AS OP_CTX_ENTRY_OPR_ID,
	LKP_Claim_Party_Occurrence_AK_ID.offset_onset_ind AS claim_occurrence_type_code,
	claim_occurrence_ak_id AS CLAIM_AK_ID_VAR,
	EXP_Default1.CTX_CLIENT_ID,
	-- *INF*: LTRIM(RTRIM(CTX_CLIENT_ID))
	-- 
	-- ---IIF(NOT ISNULL(CLAIM_AK_ID_VAR), :LKP.LKP_CLAIM_PARTY(LTRIM(RTRIM(CTX_CLIENT_ID))), NULL)
	LTRIM(RTRIM(CTX_CLIENT_ID)) AS CLAIM_PARTY_KEY,
	-- *INF*: ---IIF(NOT ISNULL(CLAIM_AK_ID_VAR), :LKP.LKP_CLAIM_PARTY(LTRIM(RTRIM(CTX_CLIENT_ID))), NULL)
	'' AS PARTY_AK_ID_VAR,
	-- *INF*: --IIF((NOT ISNULL(CLAIM_AK_ID_VAR) AND NOT ISNULL(PARTY_AK_ID_VAR)), :LKP.LKP_CLAIM_PARTY_OCCURENCE(CLAIM_AK_ID_VAR, PARTY_AK_ID_VAR), NULL)
	'' AS PARTY_OCC_AK_ID_VAR,
	EXP_Default1.CTX_OBJECT_SEQ_NBR,
	EXP_Default1.CTX_COV_TYPE_CD,
	EXP_Default1.CTX_COV_SEQ_NBR,
	-- *INF*: ---IIF(NOT ISNULL(PARTY_OCC_AK_ID_VAR),:LKP.LKP_CLAIMANT_COVERAGE_DETAIL(PARTY_OCC_AK_ID_VAR, CTX_OBJECT_TYPE_CD, CTX_OBJECT_SEQ_NBR, CTX_COV_TYPE_CD, CTX_COV_SEQ_NBR, V_CAUSE_LOSS, V_RESERVE_CAT), NULL)
	-- 
	-- --IIF(NOT ISNULL(PARTY_OCC_AK_ID_VAR),:LKP.LKP_CLAIMANT_COVERAGE_DETAIL(PARTY_OCC_AK_ID_VAR, CTX_OBJECT_TYPE_CD, CTX_OBJECT_SEQ_NBR, CTX_COV_TYPE_CD, CTX_COV_SEQ_NBR), NULL)
	'' AS OP_CLAIM_COV_DTL_AK_ID,
	1 AS Crrnt_SnapSht_Flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AUDIT_ID,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS SOURCE_SYSTEM_ID,
	-- *INF*: TO_DATE('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS')
	TO_TIMESTAMP('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS') AS eff_from_date,
	-- *INF*: TO_DATE('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS')
	TO_TIMESTAMP('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS') AS reprocess_date,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_TIMESTAMP('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS eff_to_date,
	SYSDATE AS created_date,
	SYSDATE AS modified_date,
	EXP_Default1.CTX_BASE_TRS_TYPE,
	-- *INF*: IIF(ISNULL(CTX_BASE_TRS_TYPE) ,'N/A',TO_CHAR(CTX_BASE_TRS_TYPE))
	IFF(CTX_BASE_TRS_TYPE IS NULL, 'N/A', TO_CHAR(CTX_BASE_TRS_TYPE)) AS OP_CTX_BASE_TRS_TYPE,
	EXP_Chk_Null_Lkp.op_edw_trans_ctgry_code AS CTX_TRS_CAT_CD,
	-- *INF*: IIF(ISNULL(CTX_TRS_CAT_CD) OR IS_SPACES(CTX_TRS_CAT_CD),'N/A',CTX_TRS_CAT_CD)
	IFF(
	    CTX_TRS_CAT_CD IS NULL OR LENGTH(CTX_TRS_CAT_CD)>0 AND TRIM(CTX_TRS_CAT_CD)='', 'N/A',
	    CTX_TRS_CAT_CD
	) AS OP_CTX_TRS_CAT_CD,
	EXP_Default1.CTX_CLAIM_NBR,
	LKP_Clm_Cov_Pkg_Stage.CCP_MNL_ENTRY_IND AS IP_CCP_MNL_ENTRY_IND,
	LKP_Clm_Cov_Pkg_Stage.CCP_SAR_ID AS IP_CCP_SAR_ID,
	-- *INF*: ---:LKP.LKP_CLM_OBJECT_STAGE(CTX_CLAIM_NBR,CTX_OBJECT_TYPE_CD,CTX_OBJECT_SEQ_NBR)
	'' AS COB_SAR_ID_VAR,
	-- *INF*: IIF(claim_occurrence_type_code = 'WCC','45',IP_CCP_SAR_ID)
	-- 
	-- 
	-- ----  For Worker Comp Claims we are always hard coding the sar_id to 45 
	-- 
	-- -- Changed the logic on 08/19/2010
	-- 
	-- --LTRIM(RTRIM(IIF(IP_CCP_MNL_ENTRY_IND = '1' OR claim_occurrence_type_code = --'COM',IP_CCP_SAR_ID,COB_SAR_ID_VAR)))
	IFF(claim_occurrence_type_code = 'WCC', '45', IP_CCP_SAR_ID) AS COVERAGE_SAR_ID_VAR,
	-- *INF*: IIF(ISNULL(COVERAGE_SAR_ID_VAR) OR 
	-- IS_SPACES(COVERAGE_SAR_ID_VAR),'N/A',COVERAGE_SAR_ID_VAR)
	IFF(
	    COVERAGE_SAR_ID_VAR IS NULL OR LENGTH(COVERAGE_SAR_ID_VAR)>0 AND TRIM(COVERAGE_SAR_ID_VAR)='',
	    'N/A',
	    COVERAGE_SAR_ID_VAR
	) AS OP_SAR_ID,
	'N/A' AS OP_PMS_LOSS_TRANS_CODE,
	-- *INF*: TO_DATE('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS')
	TO_TIMESTAMP('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS') AS OP_NULL_PMS_DATES,
	EXP_Default.CTX_TRS_TYPE_CD AS CTX_ORIG_TRS_TYPE_CD,
	EXP_Default1.CLAIM_TRANSACTION_ID,
	LKP_Claim_Payment.claim_pay_ak_id AS in_claim_pay_ak_id,
	-- *INF*: IIF(ISNULL(in_claim_pay_ak_id), -1, in_claim_pay_ak_id)
	IFF(in_claim_pay_ak_id IS NULL, - 1, in_claim_pay_ak_id) AS claim_pay_ak_id,
	0 AS err_flag,
	LKP_Claim_1099_Master_List_connected.irs_tax_id AS IN_LKP_IRS_TAX_ID,
	LKP_Claim_1099_Master_List_connected.tax_id AS IN_LKP_TAX_ID,
	LKP_Claim_1099_Master_List_connected.claim_master_1099_list_ak_id AS IN_LKP_MASTER_1099_AK_ID,
	-- *INF*: iif(isnull(IN_LKP_TAX_ID)  OR length(IN_LKP_TAX_ID)= 0 OR IS_SPACES(IN_LKP_TAX_ID), '000000000',ltrim(rtrim(IN_LKP_TAX_ID)))
	IFF(
	    IN_LKP_TAX_ID IS NULL
	    or length(IN_LKP_TAX_ID) = 0
	    or LENGTH(IN_LKP_TAX_ID)>0
	    and TRIM(IN_LKP_TAX_ID)='',
	    '000000000',
	    ltrim(rtrim(IN_LKP_TAX_ID))
	) AS V_TAX_ID,
	-- *INF*: iif(isnull(IN_LKP_IRS_TAX_ID)  OR length(IN_LKP_IRS_TAX_ID)= 0 OR IS_SPACES(IN_LKP_IRS_TAX_ID), '000000000',ltrim(rtrim(IN_LKP_IRS_TAX_ID)))
	IFF(
	    IN_LKP_IRS_TAX_ID IS NULL
	    or length(IN_LKP_IRS_TAX_ID) = 0
	    or LENGTH(IN_LKP_IRS_TAX_ID)>0
	    and TRIM(IN_LKP_IRS_TAX_ID)='',
	    '000000000',
	    ltrim(rtrim(IN_LKP_IRS_TAX_ID))
	) AS v_IRS_TAX_ID,
	-- *INF*: IIF(V_TAX_ID='000000000',-1,IIF(ISNULL(IN_LKP_MASTER_1099_AK_ID),-1,IN_LKP_MASTER_1099_AK_ID))
	IFF(
	    V_TAX_ID = '000000000', - 1,
	    IFF(
	        IN_LKP_MASTER_1099_AK_ID IS NULL, - 1, IN_LKP_MASTER_1099_AK_ID
	    )
	) AS V_1099_AK_ID,
	V_TAX_ID AS out_tax_id,
	V_1099_AK_ID AS out_claim_master_1099_list_ak_id,
	EXP_Default.CTX_CREATE_TS,
	-- *INF*: (CTX_FIN_TYPE_CD || TO_CHAR(CTX_ORIG_TRS_TYPE_CD) 
	-- || IIF(CTX_TRS_CAT_CD = 'N/A','',CTX_TRS_CAT_CD))
	(CTX_FIN_TYPE_CD || TO_CHAR(CTX_ORIG_TRS_TYPE_CD) || IFF(CTX_TRS_CAT_CD = 'N/A', '', CTX_TRS_CAT_CD)) AS v_Exceed_Trans_Code_to_PMS_Code,
	-- *INF*: :LKP.LKP_CLAIM_SUPPORT_01_EXCEED_PMS_TRANS_CODE(LTRIM(RTRIM(
	-- v_Exceed_Trans_Code_to_PMS_Code)))
	-- 
	-- 
	-- --- Used the backfeed logic of converting Exceed trans code to pms_trans_code when
	--  ---transactions backfeed to PMS system. We need transaction code for Loss Master data 
	LKP_CLAIM_SUPPORT_01_EXCEED_PMS_TRANS_CODE_LTRIM_RTRIM_v_Exceed_Trans_Code_to_PMS_Code.cs01_code_des AS Lkp_Exceed_Trans_Code_to_PMS,
	-- *INF*: IIF(ISNULL(Lkp_Exceed_Trans_Code_to_PMS),'N/A',Lkp_Exceed_Trans_Code_to_PMS)
	IFF(Lkp_Exceed_Trans_Code_to_PMS IS NULL, 'N/A', Lkp_Exceed_Trans_Code_to_PMS) AS PMS_Trans_Code,
	'N/A' AS trans_Offset_Onset_Ind,
	LKP_Claim_Party_Occurrence_AK_ID.pms_pol_lob_code
	FROM EXP_Chk_Null_Lkp
	 -- Manually join with EXP_Default
	 -- Manually join with EXP_Default1
	LEFT JOIN LKP_Claim_1099_Master_List_connected
	ON LKP_Claim_1099_Master_List_connected.irs_tax_id = LKP_Claim_Draft_Stage.DFT_TAX_ID_NBR
	LEFT JOIN LKP_Claim_Party_Occurrence_AK_ID
	ON LKP_Claim_Party_Occurrence_AK_ID.claimant_num = EXP_Default1.CTX_CLAIM_NBR AND LKP_Claim_Party_Occurrence_AK_ID.claim_party_role_code = EXP_Default1.CTX_CLIENT_ID
	LEFT JOIN LKP_Claim_Payment
	ON LKP_Claim_Payment.claim_pay_num = EXP_Default1.CTX_DRAFT_NBR
	LEFT JOIN LKP_Clm_Cov_Pkg_Stage
	ON LKP_Clm_Cov_Pkg_Stage.CCP_CLAIM_NBR = EXP_Default1.CTX_CLAIM_NBR AND LKP_Clm_Cov_Pkg_Stage.CCP_OBJECT_TYPE_CD = EXP_Default1.CTX_OBJECT_TYPE_CD AND LKP_Clm_Cov_Pkg_Stage.CCP_OBJECT_SEQ_NBR = EXP_Default1.CTX_OBJECT_SEQ_NBR AND LKP_Clm_Cov_Pkg_Stage.CCP_PKG_TYPE_CD = EXP_Default1.CTX_COV_TYPE_CD AND LKP_Clm_Cov_Pkg_Stage.CCP_PKG_SEQ_NBR = EXP_Default1.CTX_COV_SEQ_NBR
	LEFT JOIN LKP_CLM_OFFSET_HONOR_STAGE LKP_CLM_OFFSET_HONOR_STAGE_CTX_DRAFT_NBR
	ON LKP_CLM_OFFSET_HONOR_STAGE_CTX_DRAFT_NBR.COH_DRAFT_NBR = CTX_DRAFT_NBR

	LEFT JOIN LKP_CLAIM_SUPPORT_01_EXCEED_PMS_TRANS_CODE LKP_CLAIM_SUPPORT_01_EXCEED_PMS_TRANS_CODE_LTRIM_RTRIM_v_Exceed_Trans_Code_to_PMS_Code
	ON LKP_CLAIM_SUPPORT_01_EXCEED_PMS_TRANS_CODE_LTRIM_RTRIM_v_Exceed_Trans_Code_to_PMS_Code.cs01_code = LTRIM(RTRIM(v_Exceed_Trans_Code_to_PMS_Code))

),
LKP_Claimant_Coverage_Detail_AK_ID AS (
	SELECT
	claimant_cov_det_ak_id,
	major_peril_code,
	claim_party_occurrence_ak_id,
	s3p_object_type_code,
	s3p_object_seq_num,
	s3p_pkg_seq_num,
	cause_of_loss,
	reserve_ctgry
	FROM (
		SELECT 
		A.claimant_cov_det_ak_id as claimant_cov_det_ak_id, 
		A.claim_party_occurrence_ak_id as claim_party_occurrence_ak_id, 
		A.s3p_object_type_code as s3p_object_type_code, 
		A.s3p_object_seq_num as s3p_object_seq_num, 
		A.major_peril_code as major_peril_code, 
		A.s3p_pkg_seq_num as s3p_pkg_seq_num, 
		A.cause_of_loss as cause_of_loss, 
		A.reserve_ctgry as reserve_ctgry  
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.claimant_coverage_detail A
		WHERE A.crrnt_snpsht_flag = 1 AND source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_party_occurrence_ak_id,s3p_object_type_code,s3p_object_seq_num,major_peril_code,s3p_pkg_seq_num,cause_of_loss,reserve_ctgry ORDER BY claimant_cov_det_ak_id) = 1
),
LKP_Claim_Transaction AS (
	SELECT
	claim_trans_id,
	sar_id,
	tax_id,
	claim_master_1099_list_ak_id,
	claimant_cov_det_ak_id,
	cause_of_loss,
	reserve_ctgry,
	financial_type_code,
	trans_code,
	trans_date,
	trans_ctgry_code,
	draft_num
	FROM (
		SELECT CT.claim_trans_id AS claim_trans_id,
		       CT.sar_id AS sar_id,
			   CT.tax_id as tax_id,
		       CT.claim_master_1099_list_ak_id as claim_master_1099_list_ak_id,
		       CT.claimant_cov_det_ak_id AS claimant_cov_det_ak_id,
		       CT.cause_of_loss AS cause_of_loss,
		       CT.reserve_ctgry AS reserve_ctgry,
		       CT.financial_type_code AS financial_type_code,
		       CASE CT.trans_code
		         WHEN 91 THEN 90
		         WHEN 92 THEN 90
		         ELSE CT.trans_code
		       END  AS trans_code,
		       CT.trans_date  AS trans_date,
		       CT.trans_ctgry_code  AS trans_ctgry_code,
		       CT.draft_num as draft_num
		FROM   @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_transaction CT
		WHERE  CT.crrnt_snpsht_flag = 1
		       AND CT.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' 
		       AND CT.trans_offset_onset_ind in ('N/A','N')
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claimant_cov_det_ak_id,cause_of_loss,reserve_ctgry,financial_type_code,trans_code,trans_date,trans_ctgry_code,draft_num ORDER BY claim_trans_id DESC) = 1
),
LKP_Sup_CauseOfLoss AS (
	SELECT
	CauseOfLossId,
	LineOfBusiness,
	MajorPeril,
	CauseOfLoss
	FROM (
		SELECT 
			CauseOfLossId,
			LineOfBusiness,
			MajorPeril,
			CauseOfLoss
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_CauseOfLoss
		WHERE CurrentSnapshotFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY LineOfBusiness,MajorPeril,CauseOfLoss ORDER BY CauseOfLossId) = 1
),
LKP_Sup_Claim_Financial_Code AS (
	SELECT
	sup_claim_financial_code_id,
	financial_code
	FROM (
		SELECT 
			sup_claim_financial_code_id,
			financial_code
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_claim_financial_code
		WHERE crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY financial_code ORDER BY sup_claim_financial_code_id) = 1
),
LKP_Sup_Claim_Reserve_Category AS (
	SELECT
	sup_claim_reserve_ctgry_id,
	reserve_ctgry_code
	FROM (
		SELECT 
			sup_claim_reserve_ctgry_id,
			reserve_ctgry_code
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_claim_reserve_category
		WHERE crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY reserve_ctgry_code ORDER BY sup_claim_reserve_ctgry_id) = 1
),
LKP_Sup_Claim_Transaction_Category AS (
	SELECT
	sup_claim_trans_catetory_id,
	trans_ctgry_code
	FROM (
		SELECT 
			sup_claim_trans_catetory_id,
			trans_ctgry_code
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_claim_transaction_category
		WHERE crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY trans_ctgry_code ORDER BY sup_claim_trans_catetory_id) = 1
),
EXP_Values_Eval AS (
	SELECT
	LKP_Claim_Transaction.claim_trans_id,
	LKP_Claim_Transaction.sar_id AS lkp_sar_id,
	LKP_Claim_Transaction.tax_id AS lkp_tax_id,
	LKP_Claim_Transaction.claim_master_1099_list_ak_id AS lkp_claim_master_1099_list_ak_id,
	LKP_Claimant_Coverage_Detail_AK_ID.claimant_cov_det_ak_id AS i_claimant_cov_det_ak_id,
	-- *INF*: IIF(ISNULL(i_claimant_cov_det_ak_id),-1,i_claimant_cov_det_ak_id)
	IFF(i_claimant_cov_det_ak_id IS NULL, - 1, i_claimant_cov_det_ak_id) AS o_claimant_cov_det_ak_id,
	EXP_Source.OP_CAUSE_LOSS,
	EXP_Source.OP_RESERVE_CAT,
	EXP_Source.TYPE_DISABILITY_OP,
	EXP_Source.OP_OFFSET_ONSET_IND,
	EXP_Source.CTX_FIN_TYPE_CD,
	EXP_Source.CTX_TRS_TYPE_CD,
	EXP_Source.CTX_SORT_TS,
	EXP_Source.OP_CTX_UPD_TS AS CTX_UPD_TS,
	EXP_Source.OP_CTX_BASE_TRS_TYPE AS CTX_BASE_TRS_TYPE,
	EXP_Source.OP_CTX_TRS_CAT_CD AS CTX_TRS_CAT_CD,
	EXP_Source.OP_CTX_TRS_AMT AS CTX_TRS_AMT,
	EXP_Source.OP_CTX_TRS_HST_AMT AS CTX_TRS_HST_AMT,
	EXP_Source.OP_CTX_TRANS_REASON AS CTX_TRANS_REASON,
	EXP_Source.OP_CTX_DRAFT_NBR AS CTX_DRAFT_NBR,
	EXP_Source.OP_CTX_SINGLE_CHK_IND AS CTX_SINGLE_CHK_IND,
	EXP_Source.OP_CTX_OFS_REI_IND AS CTX_OFS_REI_IND,
	EXP_Source.OP_CTX_ENTRY_OPR_ID AS CTX_ENTRY_OPR_ID,
	EXP_Source.Crrnt_SnapSht_Flag,
	EXP_Source.AUDIT_ID,
	EXP_Source.SOURCE_SYSTEM_ID,
	EXP_Source.eff_from_date,
	EXP_Source.reprocess_date,
	EXP_Source.eff_to_date,
	EXP_Source.created_date,
	EXP_Source.modified_date,
	EXP_Source.OP_SAR_ID,
	-- *INF*: IIF(NOT ISNULL(claim_trans_id), IIF(OP_SAR_ID = 'N/A',lkp_sar_id,OP_SAR_ID),OP_SAR_ID)
	-- 
	-- ---6/10/2011  Added the logic as with updates to Transaction record, some time we may get value of 'N/A' for 
	-- ---OP_SAR_ID so we need to use the old value from lookup (lkp_sar_id).
	-- 
	IFF(
	    claim_trans_id IS NOT NULL, IFF(
	        OP_SAR_ID = 'N/A', lkp_sar_id, OP_SAR_ID
	    ),
	    OP_SAR_ID
	) AS SAR_ID_Out,
	EXP_Source.OP_PMS_LOSS_TRANS_CODE,
	EXP_Source.OP_NULL_PMS_DATES,
	EXP_Source.CTX_ORIG_TRS_TYPE_CD,
	EXP_Source.CLAIM_TRANSACTION_ID,
	EXP_Source.claim_pay_ak_id,
	EXP_Source.err_flag,
	-- *INF*: IIF(
	-- 	ISNULL(tax_id) or tax_id='000000000',
	-- 		IIF(
	-- 			 ISNULL(lkp_tax_id) or lkp_tax_id ='000000000',
	-- 				tax_id,
	-- 			lkp_tax_id
	-- 			)
	-- 		,tax_id
	-- 	)
	-- 
	-- -- If tax_id is valid then overwrite whatever value we have in the lookup.  
	-- --If not, and the lookup value is valid use the lookup, else use the default tax id value which defaults to '000000000'
	IFF(
	    tax_id IS NULL or tax_id = '000000000',
	    IFF(
	        lkp_tax_id IS NULL or lkp_tax_id = '000000000', tax_id, lkp_tax_id
	    ),
	    tax_id
	) AS v_tax_id,
	-- *INF*: IIF(
	-- 	isnull(claim_master_1099_list_ak_id) or claim_master_1099_list_ak_id=-1,		
	-- 		IIF( 
	-- 			isnull(lkp_claim_master_1099_list_ak_id)  or  lkp_claim_master_1099_list_ak_id = -1,
	-- 				claim_master_1099_list_ak_id,
	-- 			lkp_claim_master_1099_list_ak_id
	-- 			)
	-- 		,claim_master_1099_list_ak_id
	-- 	)
	-- 
	-- -- if source value is null or -1 use the lookup value if it is not equal to -1, else use the source value which defaults to -1 or a real value.
	IFF(
	    claim_master_1099_list_ak_id IS NULL or claim_master_1099_list_ak_id = - 1,
	    IFF(
	        lkp_claim_master_1099_list_ak_id IS NULL
	    or lkp_claim_master_1099_list_ak_id = - 1,
	        claim_master_1099_list_ak_id,
	        lkp_claim_master_1099_list_ak_id
	    ),
	    claim_master_1099_list_ak_id
	) AS v_claim_master_1099_list_ak_id,
	EXP_Source.out_tax_id AS tax_id,
	EXP_Source.out_claim_master_1099_list_ak_id AS claim_master_1099_list_ak_id,
	v_tax_id AS tax_id_out,
	v_claim_master_1099_list_ak_id AS claim_master_1099_list_ak_id_out,
	EXP_Source.CTX_CREATE_TS,
	EXP_Source.PMS_Trans_Code,
	EXP_Source.trans_Offset_Onset_Ind,
	EXP_Source.out_CTX_TRS_TYPE_CD,
	LKP_Sup_Claim_Reserve_Category.sup_claim_reserve_ctgry_id AS LKP_sup_claim_reserve_ctgry_id,
	-- *INF*: IIF(ISNULL(LKP_sup_claim_reserve_ctgry_id),-1,LKP_sup_claim_reserve_ctgry_id)
	IFF(LKP_sup_claim_reserve_ctgry_id IS NULL, - 1, LKP_sup_claim_reserve_ctgry_id) AS o_SupReserveCategoryCodeID,
	LKP_Sup_Claim_Financial_Code.sup_claim_financial_code_id AS LKP_sup_claim_financial_code_id,
	-- *INF*: IIF(ISNULL(LKP_sup_claim_financial_code_id),-1,LKP_sup_claim_financial_code_id)
	IFF(LKP_sup_claim_financial_code_id IS NULL, - 1, LKP_sup_claim_financial_code_id) AS o_FinancialTypeCodeID,
	-- *INF*: :LKP.LKP_SUP_CLAIM_TRANSACTION_CODE(LTRIM(RTRIM(TO_CHAR(CTX_ORIG_TRS_TYPE_CD))))
	LKP_SUP_CLAIM_TRANSACTION_CODE_LTRIM_RTRIM_TO_CHAR_CTX_ORIG_TRS_TYPE_CD.sup_claim_trans_code_id AS LKP_S3PTransactionCodeID,
	-- *INF*: :LKP.LKP_SUP_CLAIM_TRANSACTION_CODE(out_CTX_TRS_TYPE_CD)
	LKP_SUP_CLAIM_TRANSACTION_CODE_out_CTX_TRS_TYPE_CD.sup_claim_trans_code_id AS LKP_TransactionCodeID,
	-- *INF*: IIF(ISNULL(LKP_S3PTransactionCodeID),-1,LKP_S3PTransactionCodeID)
	IFF(LKP_S3PTransactionCodeID IS NULL, - 1, LKP_S3PTransactionCodeID) AS o_S3PTransactionCodeID,
	-- *INF*: IIF(ISNULL(LKP_TransactionCodeID),-1,LKP_TransactionCodeID)
	IFF(LKP_TransactionCodeID IS NULL, - 1, LKP_TransactionCodeID) AS o_TransactionCodeID,
	LKP_Sup_Claim_Transaction_Category.sup_claim_trans_catetory_id AS LKP_sup_claim_trans_catetory_id,
	-- *INF*: IIF(ISNULL(LKP_sup_claim_trans_catetory_id),-1,LKP_sup_claim_trans_catetory_id)
	IFF(LKP_sup_claim_trans_catetory_id IS NULL, - 1, LKP_sup_claim_trans_catetory_id) AS o_SupTransactionCategoryCodeID,
	LKP_Sup_CauseOfLoss.CauseOfLossId AS LKP_CauseOfLossId,
	-- *INF*: IIF(ISNULL(LKP_CauseOfLossId),-1,LKP_CauseOfLossId)
	IFF(LKP_CauseOfLossId IS NULL, - 1, LKP_CauseOfLossId) AS o_CauseOfLossID,
	-1 AS o_PMSTransactionCodeID
	FROM EXP_Source
	LEFT JOIN LKP_Claim_Transaction
	ON LKP_Claim_Transaction.claimant_cov_det_ak_id = LKP_Claimant_Coverage_Detail_AK_ID.claimant_cov_det_ak_id AND LKP_Claim_Transaction.cause_of_loss = EXP_Source.OP_CAUSE_LOSS AND LKP_Claim_Transaction.reserve_ctgry = EXP_Source.OP_RESERVE_CAT AND LKP_Claim_Transaction.financial_type_code = EXP_Source.CTX_FIN_TYPE_CD AND LKP_Claim_Transaction.trans_code = EXP_Source.OP_CTX_TRS_TYPE_CD AND LKP_Claim_Transaction.trans_date = EXP_Source.CTX_SORT_TS AND LKP_Claim_Transaction.trans_ctgry_code = EXP_Source.OP_CTX_TRS_CAT_CD AND LKP_Claim_Transaction.draft_num = EXP_Source.OP_CTX_DRAFT_NBR
	LEFT JOIN LKP_Claimant_Coverage_Detail_AK_ID
	ON LKP_Claimant_Coverage_Detail_AK_ID.claim_party_occurrence_ak_id = EXP_Source.claim_party_occurrence_ak_id AND LKP_Claimant_Coverage_Detail_AK_ID.s3p_object_type_code = EXP_Source.CTX_OBJECT_TYPE_CD AND LKP_Claimant_Coverage_Detail_AK_ID.s3p_object_seq_num = EXP_Source.CTX_OBJECT_SEQ_NBR AND LKP_Claimant_Coverage_Detail_AK_ID.major_peril_code = EXP_Source.CTX_COV_TYPE_CD AND LKP_Claimant_Coverage_Detail_AK_ID.s3p_pkg_seq_num = EXP_Source.CTX_COV_SEQ_NBR AND LKP_Claimant_Coverage_Detail_AK_ID.cause_of_loss = EXP_Source.OP_CAUSE_LOSS AND LKP_Claimant_Coverage_Detail_AK_ID.reserve_ctgry = EXP_Source.OP_RESERVE_CAT
	LEFT JOIN LKP_Sup_CauseOfLoss
	ON LKP_Sup_CauseOfLoss.LineOfBusiness = EXP_Source.pms_pol_lob_code AND LKP_Sup_CauseOfLoss.MajorPeril = LKP_Claimant_Coverage_Detail_AK_ID.major_peril_code AND LKP_Sup_CauseOfLoss.CauseOfLoss = EXP_Source.OP_CAUSE_LOSS
	LEFT JOIN LKP_Sup_Claim_Financial_Code
	ON LKP_Sup_Claim_Financial_Code.financial_code = EXP_Source.CTX_FIN_TYPE_CD
	LEFT JOIN LKP_Sup_Claim_Reserve_Category
	ON LKP_Sup_Claim_Reserve_Category.reserve_ctgry_code = EXP_Source.OP_RESERVE_CAT
	LEFT JOIN LKP_Sup_Claim_Transaction_Category
	ON LKP_Sup_Claim_Transaction_Category.trans_ctgry_code = EXP_Source.OP_CTX_TRS_CAT_CD
	LEFT JOIN LKP_SUP_CLAIM_TRANSACTION_CODE LKP_SUP_CLAIM_TRANSACTION_CODE_LTRIM_RTRIM_TO_CHAR_CTX_ORIG_TRS_TYPE_CD
	ON LKP_SUP_CLAIM_TRANSACTION_CODE_LTRIM_RTRIM_TO_CHAR_CTX_ORIG_TRS_TYPE_CD.trans_code = LTRIM(RTRIM(TO_CHAR(CTX_ORIG_TRS_TYPE_CD)))

	LEFT JOIN LKP_SUP_CLAIM_TRANSACTION_CODE LKP_SUP_CLAIM_TRANSACTION_CODE_out_CTX_TRS_TYPE_CD
	ON LKP_SUP_CLAIM_TRANSACTION_CODE_out_CTX_TRS_TYPE_CD.trans_code = out_CTX_TRS_TYPE_CD

),
RTR_Claim_Transaction AS (
	SELECT
	claim_trans_id,
	o_claimant_cov_det_ak_id AS claimant_cov_det_ak_id,
	OP_CAUSE_LOSS,
	OP_RESERVE_CAT,
	TYPE_DISABILITY_OP,
	OP_OFFSET_ONSET_IND,
	CTX_FIN_TYPE_CD,
	CTX_TRS_TYPE_CD,
	CTX_SORT_TS,
	CTX_UPD_TS,
	CTX_BASE_TRS_TYPE,
	CTX_TRS_CAT_CD,
	CTX_TRS_AMT,
	CTX_TRS_HST_AMT,
	CTX_TRANS_REASON,
	CTX_DRAFT_NBR,
	CTX_SINGLE_CHK_IND,
	CTX_OFS_REI_IND,
	CTX_ENTRY_OPR_ID,
	Crrnt_SnapSht_Flag,
	AUDIT_ID,
	SOURCE_SYSTEM_ID,
	eff_from_date,
	eff_to_date,
	created_date,
	modified_date,
	SAR_ID_Out AS OP_SAR_ID,
	OP_PMS_LOSS_TRANS_CODE,
	OP_NULL_PMS_DATES,
	CTX_ORIG_TRS_TYPE_CD,
	CLAIM_TRANSACTION_ID,
	reprocess_date,
	claim_pay_ak_id,
	err_flag,
	tax_id_out AS tax_id,
	claim_master_1099_list_ak_id_out AS claim_master_1099_list_ak_id,
	CTX_CREATE_TS,
	PMS_Trans_Code,
	trans_Offset_Onset_Ind,
	o_SupReserveCategoryCodeID AS SupReserveCategoryCodeID,
	o_FinancialTypeCodeID AS FinancialTypeCodeID,
	o_S3PTransactionCodeID AS S3PTransactionCodeID,
	o_TransactionCodeID AS TransactionCodeID,
	o_SupTransactionCategoryCodeID AS SupTransactionCategoryCodeID,
	o_CauseOfLossID AS CauseOfLossID,
	o_PMSTransactionCodeID AS PMSTransactionCodeID
	FROM EXP_Values_Eval
),
RTR_Claim_Transaction_CLAIM_TXN_UPDATE AS (SELECT * FROM RTR_Claim_Transaction WHERE NOT ISNULL(claim_trans_id)),
RTR_Claim_Transaction_DEFAULT1 AS (SELECT * FROM RTR_Claim_Transaction WHERE NOT ( (NOT ISNULL(claim_trans_id)) )),
SEQ_Claim_Transaction AS (
	CREATE SEQUENCE SEQ_Claim_Transaction
	START = 0
	INCREMENT = 1;
),
claim_transaction_insert AS (
	INSERT INTO claim_transaction
	(claim_trans_ak_id, claimant_cov_det_ak_id, claim_pay_ak_id, cause_of_loss, reserve_ctgry, type_disability, sar_id, offset_onset_ind, financial_type_code, s3p_trans_code, pms_trans_code, trans_code, trans_date, s3p_updated_date, s3p_to_pms_trans_date, pms_acct_entered_date, trans_base_type_code, trans_ctgry_code, trans_amt, trans_hist_amt, trans_rsn, draft_num, single_check_ind, offset_reissue_ind, reprocess_date, trans_entry_oper_id, wc_stage_pk_id, err_flag, crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date, tax_id, claim_master_1099_list_ak_id, trans_offset_onset_ind, s3p_created_date, CauseOfLossID, SupReserveCategoryCodeID, FinancialTypeCodeID, S3PTransactionCodeID, PMSTransactionCodeID, TransactionCodeID, SupTransactionCategoryCodeID)
	SELECT 
	SEQ_Claim_Transaction.NEXTVAL AS CLAIM_TRANS_AK_ID, 
	CLAIMANT_COV_DET_AK_ID, 
	CLAIM_PAY_AK_ID, 
	OP_CAUSE_LOSS AS CAUSE_OF_LOSS, 
	OP_RESERVE_CAT AS RESERVE_CTGRY, 
	TYPE_DISABILITY_OP AS TYPE_DISABILITY, 
	OP_SAR_ID AS SAR_ID, 
	OP_OFFSET_ONSET_IND AS OFFSET_ONSET_IND, 
	CTX_FIN_TYPE_CD AS FINANCIAL_TYPE_CODE, 
	CTX_ORIG_TRS_TYPE_CD AS S3P_TRANS_CODE, 
	PMS_Trans_Code AS PMS_TRANS_CODE, 
	CTX_TRS_TYPE_CD AS TRANS_CODE, 
	CTX_SORT_TS AS TRANS_DATE, 
	CTX_UPD_TS AS S3P_UPDATED_DATE, 
	OP_NULL_PMS_DATES AS S3P_TO_PMS_TRANS_DATE, 
	OP_NULL_PMS_DATES AS PMS_ACCT_ENTERED_DATE, 
	CTX_BASE_TRS_TYPE AS TRANS_BASE_TYPE_CODE, 
	CTX_TRS_CAT_CD AS TRANS_CTGRY_CODE, 
	CTX_TRS_AMT AS TRANS_AMT, 
	CTX_TRS_HST_AMT AS TRANS_HIST_AMT, 
	CTX_TRANS_REASON AS TRANS_RSN, 
	CTX_DRAFT_NBR AS DRAFT_NUM, 
	CTX_SINGLE_CHK_IND AS SINGLE_CHECK_IND, 
	CTX_OFS_REI_IND AS OFFSET_REISSUE_IND, 
	REPROCESS_DATE, 
	CTX_ENTRY_OPR_ID AS TRANS_ENTRY_OPER_ID, 
	CLAIM_TRANSACTION_ID AS WC_STAGE_PK_ID, 
	ERR_FLAG, 
	Crrnt_SnapSht_Flag AS CRRNT_SNPSHT_FLAG, 
	AUDIT_ID AS AUDIT_ID, 
	EFF_FROM_DATE, 
	EFF_TO_DATE, 
	SOURCE_SYSTEM_ID AS SOURCE_SYS_ID, 
	CREATED_DATE, 
	MODIFIED_DATE, 
	TAX_ID, 
	CLAIM_MASTER_1099_LIST_AK_ID, 
	trans_Offset_Onset_Ind AS TRANS_OFFSET_ONSET_IND, 
	CTX_CREATE_TS AS S3P_CREATED_DATE, 
	CAUSEOFLOSSID, 
	SUPRESERVECATEGORYCODEID, 
	FINANCIALTYPECODEID, 
	S3PTRANSACTIONCODEID, 
	PMSTRANSACTIONCODEID, 
	TRANSACTIONCODEID, 
	SUPTRANSACTIONCATEGORYCODEID
	FROM RTR_Claim_Transaction_DEFAULT1
),
UPD_Claim_Transaction AS (
	SELECT
	claim_trans_id, 
	TYPE_DISABILITY_OP AS TYPE_DISABILITY_OP1, 
	OP_OFFSET_ONSET_IND AS OP_OFFSET_ONSET_IND1, 
	CTX_UPD_TS AS CTX_UPD_TS1, 
	CTX_BASE_TRS_TYPE AS CTX_BASE_TRS_TYPE1, 
	CTX_TRS_CAT_CD AS CTX_TRS_CAT_CD1, 
	CTX_TRS_AMT AS CTX_TRS_AMT1, 
	CTX_TRS_HST_AMT AS CTX_TRS_HST_AMT1, 
	CTX_TRANS_REASON AS CTX_TRANS_REASON1, 
	CTX_DRAFT_NBR AS CTX_DRAFT_NBR1, 
	CTX_SINGLE_CHK_IND AS CTX_SINGLE_CHK_IND1, 
	CTX_OFS_REI_IND AS CTX_OFS_REI_IND1, 
	CTX_ENTRY_OPR_ID AS CTX_ENTRY_OPR_ID1, 
	AUDIT_ID AS AUDIT_ID1, 
	modified_date AS modified_date1, 
	OP_SAR_ID AS OP_SAR_ID1, 
	CLAIM_TRANSACTION_ID AS CLAIM_TRANSACTION_ID1, 
	CTX_TRS_TYPE_CD AS CTX_TRS_TYPE_CD1, 
	CTX_ORIG_TRS_TYPE_CD AS CTX_ORIG_TRS_TYPE_CD1, 
	created_date AS created_date1, 
	tax_id AS tax_id1, 
	claim_master_1099_list_ak_id AS claim_master_1099_list_ak_id1, 
	PMS_Trans_Code AS PMS_Trans_Code1, 
	SupReserveCategoryCodeID AS SupReserveCategoryCodeID1, 
	FinancialTypeCodeID AS FinancialTypeCodeID1, 
	S3PTransactionCodeID AS S3PTransactionCodeID1, 
	TransactionCodeID AS TransactionCodeID1, 
	SupTransactionCategoryCodeID AS SupTransactionCategoryCodeID1, 
	CauseOfLossID AS CauseOfLossID1, 
	PMSTransactionCodeID AS PMSTransactionCodeID1
	FROM RTR_Claim_Transaction_CLAIM_TXN_UPDATE
),
claim_transaction_update AS (
	MERGE INTO claim_transaction AS T
	USING UPD_Claim_Transaction AS S
	ON T.claim_trans_id = S.claim_trans_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.type_disability = S.TYPE_DISABILITY_OP1, T.sar_id = S.OP_SAR_ID1, T.offset_onset_ind = S.OP_OFFSET_ONSET_IND1, T.s3p_trans_code = S.CTX_ORIG_TRS_TYPE_CD1, T.pms_trans_code = S.PMS_Trans_Code1, T.trans_code = S.CTX_TRS_TYPE_CD1, T.s3p_updated_date = S.CTX_UPD_TS1, T.trans_base_type_code = S.CTX_BASE_TRS_TYPE1, T.trans_ctgry_code = S.CTX_TRS_CAT_CD1, T.trans_rsn = S.CTX_TRANS_REASON1, T.single_check_ind = S.CTX_SINGLE_CHK_IND1, T.offset_reissue_ind = S.CTX_OFS_REI_IND1, T.trans_entry_oper_id = S.CTX_ENTRY_OPR_ID1, T.audit_id = S.AUDIT_ID1, T.modified_date = S.modified_date1, T.tax_id = S.tax_id1, T.claim_master_1099_list_ak_id = S.claim_master_1099_list_ak_id1, T.CauseOfLossID = S.CauseOfLossID1, T.SupReserveCategoryCodeID = S.SupReserveCategoryCodeID1, T.FinancialTypeCodeID = S.FinancialTypeCodeID1, T.S3PTransactionCodeID = S.S3PTransactionCodeID1, T.PMSTransactionCodeID = S.PMSTransactionCodeID1, T.TransactionCodeID = S.TransactionCodeID1, T.SupTransactionCategoryCodeID = S.SupTransactionCategoryCodeID1
),
SQ_claim_transaction_92 AS (
	SELECT CT.claim_trans_id,
	       CT.claimant_cov_det_ak_id,
	       CT.cause_of_loss,
	       CT.reserve_ctgry,
	       CT.type_disability,
	       CT.s3p_trans_code,
	       CT.pms_trans_code,
	       CT.trans_code
	FROM   @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_transaction CT
	WHERE  CT.financial_type_code = 'D'
	       AND CT.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	       AND CT.s3p_trans_code = '92'
),
EXP_Default_1 AS (
	SELECT
	claim_trans_id,
	claimant_cov_det_ak_id,
	cause_of_loss,
	reserve_ctgry,
	type_disability,
	s3p_trans_code,
	pms_trans_code,
	trans_code
	FROM SQ_claim_transaction_92
),
LKP_Claim_Transaction_EXD_66 AS (
	SELECT
	claim_trans_id,
	claim_trans_ak_id,
	claimant_cov_det_ak_id,
	cause_of_loss,
	reserve_ctgry,
	type_disability
	FROM (
		SELECT 
		       CT1.claim_trans_id         AS claim_trans_id,
		       CT1.claim_trans_ak_id      AS claim_trans_ak_id,
		       CT1.claimant_cov_det_ak_id AS claimant_cov_det_ak_id,
		       RTRIM(CT1.cause_of_loss)          AS cause_of_loss,
		       RTRIM(CT1.reserve_ctgry)          AS reserve_ctgry,
		       RTRIM(CT1.type_disability)        AS type_disability 
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_transaction CT1          
		WHERE CT1.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
		AND CT1.pms_trans_code  = '66'
		AND CT1.financial_type_code = 'D'
		AND 
		       EXISTS 
		                      ( SELECT 1 FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_transaction CT2
		                        WHERE CT2.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
		                        AND CT2.pms_trans_code in ('23','22','42')
		                        AND CT2.financial_type_code = 'D'
		                        AND CT2.claimant_cov_det_ak_id = CT1.claimant_cov_det_ak_id)
		
		---- Above rules are mimicing the PMS Logic from Program PMS723.PRD1.SOURCE(WDD0900), paragraph 0217, where it coverts 90 trans code to 92
		---- based on existence of some transcations 23,22,42,66 for Coverage of that claim.
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claimant_cov_det_ak_id,cause_of_loss,reserve_ctgry,type_disability ORDER BY claim_trans_id DESC) = 1
),
EXP_Values AS (
	SELECT
	EXP_Default_1.claim_trans_id,
	LKP_Claim_Transaction_EXD_66.claim_trans_id AS LKP_claim_trans_id,
	-- *INF*: IIF(NOT ISNULL(LKP_claim_trans_id),'92','90')
	-- 
	-- ---- From the lookup if we get a matching condition like for the combination of Cov_det_ak_id,cause_of_loss,reserve_ctgry there is 92 and 66 then we update the pms_trans_code of EXCEED transaction to 92 otherwise it will be 90
	IFF(LKP_claim_trans_id IS NOT NULL, '92', '90') AS New_PMS_trans_code_92,
	LKP_Claim_Transaction_EXD_66.claim_trans_ak_id
	FROM EXP_Default_1
	LEFT JOIN LKP_Claim_Transaction_EXD_66
	ON LKP_Claim_Transaction_EXD_66.claimant_cov_det_ak_id = EXP_Default_1.claimant_cov_det_ak_id AND LKP_Claim_Transaction_EXD_66.cause_of_loss = EXP_Default_1.cause_of_loss AND LKP_Claim_Transaction_EXD_66.reserve_ctgry = EXP_Default_1.reserve_ctgry AND LKP_Claim_Transaction_EXD_66.type_disability = EXP_Default_1.type_disability
),
UPD_Claim_Transaction_EXD AS (
	SELECT
	claim_trans_id, 
	New_PMS_trans_code_92
	FROM EXP_Values
),
claim_transaction_UPD_92 AS (
	MERGE INTO claim_transaction AS T
	USING UPD_Claim_Transaction_EXD AS S
	ON T.claim_trans_id = S.claim_trans_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.pms_trans_code = S.New_PMS_trans_code_92
),