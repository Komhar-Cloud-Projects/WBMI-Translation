WITH
SQ_CLAIM_TRANSACTION_STAGE AS (
	SELECT DISTINCT A.ctx_claim_nbr, A.ctx_client_id, A.ctx_object_type_cd, A.ctx_object_seq_nbr, A.ctx_cov_type_cd, A.ctx_cov_seq_nbr, A.ctx_bur_cause_loss 
	FROM
	 claim_transaction_stage A
),
EXPTRANS AS (
	SELECT
	ctx_claim_nbr,
	ctx_client_id,
	-- *INF*: LTRIM(RTRIM(ctx_client_id))
	LTRIM(RTRIM(ctx_client_id)) AS ctx_client_id_out,
	ctx_object_type_cd,
	-- *INF*: LTRIM(RTRIM(ctx_object_type_cd))
	-- 
	-- --IIF(IS_SPACES(ctx_object_type_cd) OR ISNULL(ctx_object_type_cd),'N/A',LTRIM(RTRIM(ctx_object_type_cd)))
	LTRIM(RTRIM(ctx_object_type_cd)) AS ctx_object_type_cd_out,
	ctx_object_seq_nbr,
	-- *INF*: --IIF(ISNULL(ctx_object_seq_nbr)
	-- --,0,
	-- ctx_object_seq_nbr
	-- --)
	ctx_object_seq_nbr AS ctx_object_seq_nbr_out,
	ctx_cov_type_cd,
	-- *INF*: --IIF(IS_SPACES(ctx_cov_type_cd) OR ISNULL(ctx_cov_type_cd),'N/A',
	-- LTRIM(RTRIM(ctx_cov_type_cd))
	-- --)
	LTRIM(RTRIM(ctx_cov_type_cd)) AS ctx_cov_type_cd_out,
	ctx_cov_seq_nbr,
	-- *INF*: --IIF(ISNULL(ctx_cov_seq_nbr),0,
	-- ctx_cov_seq_nbr
	-- --)
	ctx_cov_seq_nbr AS ctx_cov_seq_nbr_out,
	ctx_bur_cause_loss
	FROM SQ_CLAIM_TRANSACTION_STAGE
),
SQ_CLAIM_OBJECT_CLT_STAGE AS (
	SELECT distinct
	  a.CCT_CLAIM_NBR
	, a.CCT_CLIENT_ID 
	FROM
	 @{pipeline().parameters.SOURCE_TABLE_OWNER}.CLAIM_OBJECT_CLT_STAGE a
	WHERE a.CCT_CLIENT_ROLE_CD = 'CLMT'
),
EXP_Claim_Object_Clt AS (
	SELECT
	CCT_CLAIM_NBR,
	-- *INF*: ltrim(rtrim(CCT_CLAIM_NBR))
	ltrim(rtrim(CCT_CLAIM_NBR)) AS out_CCT_CLAIM_NBR,
	CCT_CLIENT_ID,
	-- *INF*: ltrim(rtrim(CCT_CLIENT_ID))
	ltrim(rtrim(CCT_CLIENT_ID)) AS out_CCT_CLIENT_ID
	FROM SQ_CLAIM_OBJECT_CLT_STAGE
),
SQ_CLM_COV_PKG_STAGE AS (
	SELECT DISTINCT A.CCP_CLAIM_NBR
		,A.CCP_OBJECT_TYPE_CD
		,A.CCP_OBJECT_SEQ_NBR
		,A.CCP_PKG_TYPE_CD
		,A.CCP_PKG_SEQ_NBR
		,A.CCP_PKG_DED_AMT
		,A.CCP_PKG_EFF_DT
		,A.CCP_PKG_EXP_DT
		,A.CCP_PKG_LIMIT_AMT
		,A.CCP_MNL_ENTRY_IND
		,A.CCP_INS_LINE_CD
		,A.CCP_MAJR_PERIL_SEQ
		,A.CCP_SAR_ID
		,A.CCP_INS_LINE
		,A.CCP_LOC_UNIT_NUM
		,A.CCP_RISK_UNIT_GRP
		,A.CCP_RSK_UNT_GR_SEQ
		,A.CCP_RISK_UNIT
		,A.CCP_RISK_TYPE_IND
		,A.CCP_SUB_LOC_NUM
		,A.CCP_SEQ_RISK_UNIT
		,A.ccp_coverage_form
		,A.ccp_coverage_type
		,A.ccp_risk_type
		,CASE LTRIM(RTRIM(B.cvr_policy_src_id)) WHEN 'PDC' THEN 'DUC' ELSE B.cvr_policy_src_id END AS cvr_policy_src_id
		,A.ccp_pol_cov_id
	FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.CLM_COV_PKG_STAGE  A 
	LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_coverage_stage B
	ON A.CCP_CLAIM_NBR = B.cvr_claim_nbr
),
EXP_CLM_COV_PKG AS (
	SELECT
	CCP_CLAIM_NBR,
	CCP_OBJECT_TYPE_CD,
	CCP_OBJECT_SEQ_NBR,
	CCP_PKG_TYPE_CD,
	-- *INF*: IIF(IS_SPACES(CCP_PKG_TYPE_CD) OR ISNULL(CCP_PKG_TYPE_CD)
	-- ,'N/A'
	-- ,LTRIM(RTRIM(CCP_PKG_TYPE_CD)))
	IFF(
	    LENGTH(CCP_PKG_TYPE_CD)>0 AND TRIM(CCP_PKG_TYPE_CD)='' OR CCP_PKG_TYPE_CD IS NULL, 'N/A',
	    LTRIM(RTRIM(CCP_PKG_TYPE_CD))
	) AS out_PKG_TYPE_CD,
	CCP_PKG_SEQ_NBR,
	-- *INF*: IIF(ISNULL(CCP_PKG_SEQ_NBR)
	-- ,0
	-- ,CCP_PKG_SEQ_NBR)
	IFF(CCP_PKG_SEQ_NBR IS NULL, 0, CCP_PKG_SEQ_NBR) AS out_PKG_SEQ_NBR,
	CCP_PKG_DED_AMT,
	CCP_PKG_EFF_DT,
	CCP_PKG_EXP_DT,
	CCP_PKG_LIMIT_AMT,
	CCP_MNL_ENTRY_IND,
	-- *INF*: IIF(IS_SPACES(CCP_MNL_ENTRY_IND) OR ISNULL(CCP_MNL_ENTRY_IND)
	-- ,'N/A'
	-- ,ltrim(rtrim(CCP_MNL_ENTRY_IND)))
	IFF(
	    LENGTH(CCP_MNL_ENTRY_IND)>0 AND TRIM(CCP_MNL_ENTRY_IND)='' OR CCP_MNL_ENTRY_IND IS NULL,
	    'N/A',
	    ltrim(rtrim(CCP_MNL_ENTRY_IND))
	) AS out_CCP_MNL_ENTRY_IND,
	CCP_INS_LINE_CD,
	-- *INF*: IIF(IS_SPACES(CCP_INS_LINE_CD) OR ISNULL(CCP_INS_LINE_CD)
	-- ,'N/A'
	-- ,CCP_INS_LINE_CD)
	IFF(
	    LENGTH(CCP_INS_LINE_CD)>0 AND TRIM(CCP_INS_LINE_CD)='' OR CCP_INS_LINE_CD IS NULL, 'N/A',
	    CCP_INS_LINE_CD
	) AS out_CCP_INS_LINE_CD,
	CCP_MAJR_PERIL_SEQ,
	CCP_SAR_ID,
	CCP_INS_LINE,
	CCP_LOC_UNIT_NUM,
	CCP_RISK_UNIT_GRP,
	CCP_RSK_UNT_GR_SEQ,
	CCP_RISK_UNIT,
	CCP_RISK_TYPE_IND,
	CCP_SUB_LOC_NUM,
	CCP_SEQ_RISK_UNIT,
	ccp_coverage_form,
	ccp_coverage_type,
	-- *INF*: LTRIM(RTRIM(ccp_coverage_type))
	LTRIM(RTRIM(ccp_coverage_type)) AS o_ccp_coverage_type,
	ccp_risk_type,
	cvr_policy_src_id,
	ccp_pol_cov_id
	FROM SQ_CLM_COV_PKG_STAGE
),
SQ_CLAIM_OBJECT_STAGE AS (
	SELECT DISTINCT
	a.COB_CLAIM_NBR
	, a.COB_OBJECT_TYPE_CD
	, a.COB_OBJECT_SEQ_NBR
	, a.COB_OBJECT_CMT_ID
	, a.COB_UNIT_DES_ID
	, a.COB_COV_UNIT_NBR
	, a.COB_UNIT_TYPE_CD
	, a.COB_SPP_USE_CD
	, a.COB_SAR_ID
	, a.COB_INS_LINE
	, a.COB_LOC_UNIT_NUM
	, a.COB_RISK_UNIT_GRP
	, a.COB_RSK_UNT_GR_SEQ
	, a.COB_RISK_UNIT
	, a.COB_RISK_TYPE_IND
	, a.COB_SUB_LOC_NUM
	, a.COB_SEQ_RISK_UNIT
	, a.COB_SR_SEQ
	, a.COB_CLIENT_ID
	,a.cob_coverage_form
	,a.cob_coverage_type
	,a.cob_risk_type
	FROM
	 @{pipeline().parameters.SOURCE_TABLE_OWNER}.CLAIM_OBJECT_STAGE a
),
EXP_Claim_Coverage AS (
	SELECT
	COB_CLAIM_NBR,
	-- *INF*: ltrim(rtrim(COB_CLAIM_NBR))
	ltrim(rtrim(COB_CLAIM_NBR)) AS out_COB_CLAIM_NBR,
	COB_OBJECT_TYPE_CD,
	-- *INF*: IIF(IS_SPACES(COB_OBJECT_TYPE_CD) OR ISNULL(COB_OBJECT_TYPE_CD)
	-- ,'N/A'
	-- ,LTRIM(RTRIM(COB_OBJECT_TYPE_CD)))
	IFF(
	    LENGTH(COB_OBJECT_TYPE_CD)>0 AND TRIM(COB_OBJECT_TYPE_CD)='' OR COB_OBJECT_TYPE_CD IS NULL,
	    'N/A',
	    LTRIM(RTRIM(COB_OBJECT_TYPE_CD))
	) AS out_OBJECT_TYPE_CD,
	COB_OBJECT_SEQ_NBR,
	-- *INF*: IIF(ISNULL(COB_OBJECT_SEQ_NBR)
	-- ,0
	-- ,COB_OBJECT_SEQ_NBR)
	IFF(COB_OBJECT_SEQ_NBR IS NULL, 0, COB_OBJECT_SEQ_NBR) AS out_OBJECT_SEQ_NBR,
	COB_OBJECT_CMT_ID,
	COB_UNIT_DES_ID,
	COB_COV_UNIT_NBR,
	COB_UNIT_TYPE_CD,
	-- *INF*: IIF(IS_SPACES(COB_UNIT_TYPE_CD) OR ISNULL(COB_UNIT_TYPE_CD)
	-- ,'N/A'
	-- ,COB_UNIT_TYPE_CD)
	IFF(
	    LENGTH(COB_UNIT_TYPE_CD)>0 AND TRIM(COB_UNIT_TYPE_CD)='' OR COB_UNIT_TYPE_CD IS NULL, 'N/A',
	    COB_UNIT_TYPE_CD
	) AS out_COB_UNIT_TYPE_CD,
	COB_SPP_USE_CD,
	-- *INF*: IIF(IS_SPACES(COB_SPP_USE_CD) OR ISNULL(COB_SPP_USE_CD)
	-- ,'N/A'
	-- ,LTRIM(RTRIM(COB_SPP_USE_CD)))
	IFF(
	    LENGTH(COB_SPP_USE_CD)>0 AND TRIM(COB_SPP_USE_CD)='' OR COB_SPP_USE_CD IS NULL, 'N/A',
	    LTRIM(RTRIM(COB_SPP_USE_CD))
	) AS out_SPP_USE_CD,
	COB_SAR_ID,
	COB_INS_LINE,
	COB_LOC_UNIT_NUM,
	COB_RISK_UNIT_GRP,
	COB_RSK_UNT_GR_SEQ,
	COB_RISK_UNIT,
	COB_RISK_TYPE_IND,
	COB_SUB_LOC_NUM,
	COB_SEQ_RISK_UNIT,
	COB_SR_SEQ,
	COB_CLIENT_ID,
	-- *INF*: ltrim(rtrim(COB_CLIENT_ID))
	ltrim(rtrim(COB_CLIENT_ID)) AS out_COB_CLIENT_ID,
	cob_coverage_form,
	-- *INF*: LTRIM(RTRIM(cob_coverage_form))
	LTRIM(RTRIM(cob_coverage_form)) AS o_cob_coverage_form,
	cob_coverage_type,
	-- *INF*: LTRIM(RTRIM(cob_coverage_type))
	LTRIM(RTRIM(cob_coverage_type)) AS o_cob_coverage_type,
	cob_risk_type,
	-- *INF*: LTRIM(RTRIM(cob_risk_type))
	LTRIM(RTRIM(cob_risk_type)) AS o_cob_risk_type
	FROM SQ_CLAIM_OBJECT_STAGE
),
JNR_COVERAGE_PKG AS (SELECT
	EXP_CLM_COV_PKG.CCP_CLAIM_NBR, 
	EXP_CLM_COV_PKG.CCP_OBJECT_TYPE_CD, 
	EXP_CLM_COV_PKG.CCP_OBJECT_SEQ_NBR, 
	EXP_CLM_COV_PKG.out_PKG_TYPE_CD, 
	EXP_CLM_COV_PKG.out_PKG_SEQ_NBR, 
	EXP_CLM_COV_PKG.CCP_PKG_DED_AMT, 
	EXP_CLM_COV_PKG.CCP_PKG_EFF_DT, 
	EXP_CLM_COV_PKG.CCP_PKG_EXP_DT, 
	EXP_CLM_COV_PKG.CCP_PKG_LIMIT_AMT, 
	EXP_CLM_COV_PKG.out_CCP_MNL_ENTRY_IND, 
	EXP_CLM_COV_PKG.out_CCP_INS_LINE_CD, 
	EXP_CLM_COV_PKG.CCP_MAJR_PERIL_SEQ, 
	EXP_CLM_COV_PKG.CCP_SAR_ID, 
	EXP_CLM_COV_PKG.CCP_INS_LINE, 
	EXP_CLM_COV_PKG.CCP_LOC_UNIT_NUM, 
	EXP_CLM_COV_PKG.CCP_RISK_UNIT_GRP, 
	EXP_CLM_COV_PKG.CCP_RSK_UNT_GR_SEQ, 
	EXP_CLM_COV_PKG.CCP_RISK_UNIT, 
	EXP_CLM_COV_PKG.CCP_RISK_TYPE_IND, 
	EXP_CLM_COV_PKG.CCP_SUB_LOC_NUM, 
	EXP_CLM_COV_PKG.CCP_SEQ_RISK_UNIT, 
	EXP_Claim_Coverage.out_COB_CLAIM_NBR AS COB_CLAIM_NBR, 
	EXP_Claim_Coverage.out_OBJECT_TYPE_CD, 
	EXP_Claim_Coverage.out_OBJECT_SEQ_NBR, 
	EXP_Claim_Coverage.COB_OBJECT_CMT_ID, 
	EXP_Claim_Coverage.COB_UNIT_DES_ID, 
	EXP_Claim_Coverage.COB_COV_UNIT_NBR, 
	EXP_Claim_Coverage.out_COB_UNIT_TYPE_CD, 
	EXP_Claim_Coverage.out_SPP_USE_CD, 
	EXP_Claim_Coverage.COB_SAR_ID, 
	EXP_Claim_Coverage.COB_INS_LINE, 
	EXP_Claim_Coverage.COB_LOC_UNIT_NUM, 
	EXP_Claim_Coverage.COB_RISK_UNIT_GRP, 
	EXP_Claim_Coverage.COB_RSK_UNT_GR_SEQ, 
	EXP_Claim_Coverage.COB_RISK_UNIT, 
	EXP_Claim_Coverage.COB_RISK_TYPE_IND, 
	EXP_Claim_Coverage.COB_SUB_LOC_NUM, 
	EXP_Claim_Coverage.COB_SEQ_RISK_UNIT, 
	EXP_Claim_Coverage.COB_SR_SEQ, 
	EXP_Claim_Coverage.out_COB_CLIENT_ID AS COB_CLIENT_ID, 
	EXP_Claim_Coverage.o_cob_coverage_form AS cob_coverage_form, 
	EXP_Claim_Coverage.o_cob_coverage_type AS cob_coverage_type, 
	EXP_Claim_Coverage.o_cob_risk_type AS cob_risk_type, 
	EXP_CLM_COV_PKG.ccp_coverage_form, 
	EXP_CLM_COV_PKG.o_ccp_coverage_type AS ccp_coverage_type, 
	EXP_CLM_COV_PKG.ccp_risk_type, 
	EXP_CLM_COV_PKG.cvr_policy_src_id, 
	EXP_CLM_COV_PKG.ccp_pol_cov_id
	FROM EXP_CLM_COV_PKG
	INNER JOIN EXP_Claim_Coverage
	ON EXP_Claim_Coverage.out_COB_CLAIM_NBR = EXP_CLM_COV_PKG.CCP_CLAIM_NBR AND EXP_Claim_Coverage.out_OBJECT_TYPE_CD = EXP_CLM_COV_PKG.CCP_OBJECT_TYPE_CD AND EXP_Claim_Coverage.out_OBJECT_SEQ_NBR = EXP_CLM_COV_PKG.CCP_OBJECT_SEQ_NBR
),
JNR_COVERAGE_CLIENT AS (SELECT
	JNR_COVERAGE_PKG.COB_CLAIM_NBR, 
	JNR_COVERAGE_PKG.out_OBJECT_TYPE_CD, 
	JNR_COVERAGE_PKG.out_OBJECT_SEQ_NBR, 
	JNR_COVERAGE_PKG.COB_OBJECT_CMT_ID, 
	JNR_COVERAGE_PKG.COB_UNIT_DES_ID, 
	JNR_COVERAGE_PKG.COB_COV_UNIT_NBR, 
	JNR_COVERAGE_PKG.out_COB_UNIT_TYPE_CD AS COB_UNIT_TYPE_CD, 
	JNR_COVERAGE_PKG.out_SPP_USE_CD, 
	JNR_COVERAGE_PKG.COB_SAR_ID, 
	JNR_COVERAGE_PKG.COB_INS_LINE, 
	JNR_COVERAGE_PKG.COB_LOC_UNIT_NUM, 
	JNR_COVERAGE_PKG.COB_RISK_UNIT_GRP, 
	JNR_COVERAGE_PKG.COB_RSK_UNT_GR_SEQ, 
	JNR_COVERAGE_PKG.COB_RISK_UNIT, 
	JNR_COVERAGE_PKG.COB_RISK_TYPE_IND, 
	JNR_COVERAGE_PKG.COB_SUB_LOC_NUM, 
	JNR_COVERAGE_PKG.COB_SEQ_RISK_UNIT, 
	JNR_COVERAGE_PKG.COB_SR_SEQ, 
	JNR_COVERAGE_PKG.COB_CLIENT_ID, 
	JNR_COVERAGE_PKG.out_PKG_TYPE_CD, 
	JNR_COVERAGE_PKG.out_PKG_SEQ_NBR, 
	JNR_COVERAGE_PKG.CCP_PKG_DED_AMT, 
	JNR_COVERAGE_PKG.CCP_PKG_EFF_DT, 
	JNR_COVERAGE_PKG.CCP_PKG_EXP_DT, 
	JNR_COVERAGE_PKG.CCP_PKG_LIMIT_AMT, 
	JNR_COVERAGE_PKG.out_CCP_MNL_ENTRY_IND, 
	JNR_COVERAGE_PKG.out_CCP_INS_LINE_CD, 
	JNR_COVERAGE_PKG.CCP_MAJR_PERIL_SEQ, 
	JNR_COVERAGE_PKG.CCP_SAR_ID, 
	JNR_COVERAGE_PKG.CCP_INS_LINE, 
	JNR_COVERAGE_PKG.CCP_LOC_UNIT_NUM, 
	JNR_COVERAGE_PKG.CCP_RISK_UNIT_GRP, 
	JNR_COVERAGE_PKG.CCP_RSK_UNT_GR_SEQ, 
	JNR_COVERAGE_PKG.CCP_RISK_UNIT, 
	JNR_COVERAGE_PKG.CCP_RISK_TYPE_IND, 
	JNR_COVERAGE_PKG.CCP_SUB_LOC_NUM, 
	JNR_COVERAGE_PKG.CCP_SEQ_RISK_UNIT, 
	EXP_Claim_Object_Clt.out_CCT_CLAIM_NBR AS CCT_CLAIM_NBR, 
	EXP_Claim_Object_Clt.out_CCT_CLIENT_ID AS CCT_CLIENT_ID, 
	JNR_COVERAGE_PKG.cob_coverage_form, 
	JNR_COVERAGE_PKG.cob_coverage_type, 
	JNR_COVERAGE_PKG.cob_risk_type, 
	JNR_COVERAGE_PKG.ccp_coverage_form, 
	JNR_COVERAGE_PKG.ccp_coverage_type, 
	JNR_COVERAGE_PKG.ccp_risk_type, 
	JNR_COVERAGE_PKG.cvr_policy_src_id, 
	JNR_COVERAGE_PKG.ccp_pol_cov_id
	FROM EXP_Claim_Object_Clt
	INNER JOIN JNR_COVERAGE_PKG
	ON JNR_COVERAGE_PKG.COB_CLAIM_NBR = EXP_Claim_Object_Clt.out_CCT_CLAIM_NBR
),
JNR_coverage_transaction AS (SELECT
	JNR_COVERAGE_CLIENT.COB_CLAIM_NBR, 
	JNR_COVERAGE_CLIENT.out_OBJECT_TYPE_CD, 
	JNR_COVERAGE_CLIENT.out_OBJECT_SEQ_NBR, 
	JNR_COVERAGE_CLIENT.COB_OBJECT_CMT_ID, 
	JNR_COVERAGE_CLIENT.COB_UNIT_DES_ID, 
	JNR_COVERAGE_CLIENT.COB_COV_UNIT_NBR, 
	JNR_COVERAGE_CLIENT.COB_UNIT_TYPE_CD, 
	JNR_COVERAGE_CLIENT.out_SPP_USE_CD, 
	JNR_COVERAGE_CLIENT.COB_SAR_ID, 
	JNR_COVERAGE_CLIENT.COB_INS_LINE, 
	JNR_COVERAGE_CLIENT.COB_LOC_UNIT_NUM, 
	JNR_COVERAGE_CLIENT.COB_RISK_UNIT_GRP, 
	JNR_COVERAGE_CLIENT.COB_RSK_UNT_GR_SEQ, 
	JNR_COVERAGE_CLIENT.COB_RISK_UNIT, 
	JNR_COVERAGE_CLIENT.COB_RISK_TYPE_IND, 
	JNR_COVERAGE_CLIENT.COB_SUB_LOC_NUM, 
	JNR_COVERAGE_CLIENT.COB_SEQ_RISK_UNIT, 
	JNR_COVERAGE_CLIENT.COB_SR_SEQ, 
	JNR_COVERAGE_CLIENT.COB_CLIENT_ID, 
	JNR_COVERAGE_CLIENT.out_PKG_TYPE_CD, 
	JNR_COVERAGE_CLIENT.out_PKG_SEQ_NBR, 
	JNR_COVERAGE_CLIENT.CCP_PKG_DED_AMT, 
	JNR_COVERAGE_CLIENT.CCP_PKG_EFF_DT, 
	JNR_COVERAGE_CLIENT.CCP_PKG_EXP_DT, 
	JNR_COVERAGE_CLIENT.CCP_PKG_LIMIT_AMT, 
	JNR_COVERAGE_CLIENT.out_CCP_MNL_ENTRY_IND, 
	JNR_COVERAGE_CLIENT.out_CCP_INS_LINE_CD, 
	JNR_COVERAGE_CLIENT.CCP_MAJR_PERIL_SEQ, 
	JNR_COVERAGE_CLIENT.CCP_SAR_ID, 
	JNR_COVERAGE_CLIENT.CCP_INS_LINE, 
	JNR_COVERAGE_CLIENT.CCP_LOC_UNIT_NUM, 
	JNR_COVERAGE_CLIENT.CCP_RISK_UNIT_GRP, 
	JNR_COVERAGE_CLIENT.CCP_RSK_UNT_GR_SEQ, 
	JNR_COVERAGE_CLIENT.CCP_RISK_UNIT, 
	JNR_COVERAGE_CLIENT.CCP_RISK_TYPE_IND, 
	JNR_COVERAGE_CLIENT.CCP_SUB_LOC_NUM, 
	JNR_COVERAGE_CLIENT.CCP_SEQ_RISK_UNIT, 
	JNR_COVERAGE_CLIENT.CCT_CLAIM_NBR, 
	JNR_COVERAGE_CLIENT.CCT_CLIENT_ID, 
	EXPTRANS.ctx_claim_nbr, 
	EXPTRANS.ctx_client_id_out AS ctx_client_id, 
	EXPTRANS.ctx_object_type_cd_out AS ctx_object_type_cd, 
	EXPTRANS.ctx_object_seq_nbr_out AS ctx_object_seq_nbr, 
	EXPTRANS.ctx_cov_type_cd_out AS ctx_cov_type_cd, 
	EXPTRANS.ctx_cov_seq_nbr_out AS ctx_cov_seq_nbr, 
	EXPTRANS.ctx_bur_cause_loss, 
	JNR_COVERAGE_CLIENT.cob_coverage_form, 
	JNR_COVERAGE_CLIENT.cob_coverage_type, 
	JNR_COVERAGE_CLIENT.cob_risk_type, 
	JNR_COVERAGE_CLIENT.ccp_coverage_form, 
	JNR_COVERAGE_CLIENT.ccp_coverage_type, 
	JNR_COVERAGE_CLIENT.ccp_risk_type, 
	JNR_COVERAGE_CLIENT.cvr_policy_src_id, 
	JNR_COVERAGE_CLIENT.ccp_pol_cov_id
	FROM EXPTRANS
	INNER JOIN JNR_COVERAGE_CLIENT
	ON JNR_COVERAGE_CLIENT.COB_CLAIM_NBR = EXPTRANS.ctx_claim_nbr AND JNR_COVERAGE_CLIENT.CCT_CLIENT_ID = EXPTRANS.ctx_client_id_out AND JNR_COVERAGE_CLIENT.out_OBJECT_TYPE_CD = EXPTRANS.ctx_object_type_cd_out AND JNR_COVERAGE_CLIENT.out_OBJECT_SEQ_NBR = EXPTRANS.ctx_object_seq_nbr_out AND JNR_COVERAGE_CLIENT.out_PKG_TYPE_CD = EXPTRANS.ctx_cov_type_cd_out AND JNR_COVERAGE_CLIENT.out_PKG_SEQ_NBR = EXPTRANS.ctx_cov_seq_nbr_out
),
LKP_Auto_Loss_Stage AS (
	SELECT
	CAU_CAR_YR,
	CAU_CAR_MAKE_NM,
	CAU_CAR_RGS_NBR,
	CAU_VEHICLE_ID_NBR,
	CAU_RGS_STATE_CD,
	CAU_VEH_ST_AMT,
	CAU_CLAIM_NBR,
	CAU_OBJECT_SEQ_NBR
	FROM (
		SELECT 
		  a.CAU_CAR_YR as CAU_CAR_YR
		, a.CAU_CAR_MAKE_NM as CAU_CAR_MAKE_NM
		, a.CAU_CAR_RGS_NBR as CAU_CAR_RGS_NBR
		, a.CAU_VEHICLE_ID_NBR as CAU_VEHICLE_ID_NBR
		, a.CAU_RGS_STATE_CD as CAU_RGS_STATE_CD
		, a.CAU_VEH_ST_AMT as CAU_VEH_ST_AMT
		, Rtrim(Ltrim(a.CAU_CLAIM_NBR)) as CAU_CLAIM_NBR
		, a.CAU_OBJECT_SEQ_NBR as CAU_OBJECT_SEQ_NBR 
		FROM 
		@{pipeline().parameters.SOURCE_TABLE_OWNER}.AUTO_LOSS_STAGE a
		where a.cau_object_type_cd = 'AUT'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY CAU_CLAIM_NBR,CAU_OBJECT_SEQ_NBR ORDER BY CAU_CAR_YR) = 1
),
LKP_Claim_Comments_Stage_Unit_Desc AS (
	SELECT
	TCC_COMMENT_TXT,
	FOLDER_KEY,
	COMMENT_ITEM_NBR
	FROM (
		SELECT A.TCC_COMMENT_TXT AS TCC_COMMENT_TXT,  Rtrim(Ltrim(A.FOLDER_KEY)) AS FOLDER_KEY,  
		A.COMMENT_ITEM_NBR AS COMMENT_ITEM_NBR 
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.CLM_COMMENTS_STAGE A  
		WHERE 
		A.FOLDER_KEY in (SELECT DISTINCT B.COB_CLAIM_NBR FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.CLAIM_OBJECT_STAGE B)
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY FOLDER_KEY,COMMENT_ITEM_NBR ORDER BY TCC_COMMENT_TXT) = 1
),
EXP_Client_ID AS (
	SELECT
	CCT_CLIENT_ID,
	COB_CLIENT_ID,
	COB_INS_LINE,
	-- *INF*: IIF(UPPER(LTRIM(RTRIM(COB_INS_LINE))) != 'WC'and (isnull(COB_CLIENT_ID) or is_spaces(COB_CLIENT_ID))
	--    ,CCT_CLIENT_ID
	--    ,COB_CLIENT_ID)
	IFF(
	    UPPER(LTRIM(RTRIM(COB_INS_LINE))) != 'WC'
	    and (COB_CLIENT_ID IS NULL
	    or LENGTH(COB_CLIENT_ID)>0
	    and TRIM(COB_CLIENT_ID)=''),
	    CCT_CLIENT_ID,
	    COB_CLIENT_ID
	) AS out_CLIENT_ID
	FROM JNR_coverage_transaction
),
LKP_Claim_Party_Occurrence_AK_ID AS (
	SELECT
	claim_party_occurrence_ak_id,
	offset_onset_ind,
	claimant_num,
	claim_party_role_code
	FROM (
		SELECT CPO.claim_party_occurrence_ak_id as claim_party_occurrence_ak_id, 
		CO.claim_occurrence_type_code as offset_onset_ind,
		LTRIM(RTRIM(CO.claim_occurrence_key)) as claimant_num, 
		LTRIM(RTRIM(CP.claim_party_key)) as claim_party_role_code
		FROM 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.claim_party_occurrence CPO,
		@{pipeline().parameters.TARGET_TABLE_OWNER}.claim_party CP, 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.claim_occurrence CO 
		WHERE CO.claim_occurrence_ak_id = CPO.claim_occurrence_ak_id  AND CP.claim_party_ak_id = CPO.claim_party_ak_id 
		AND CO.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' 
		AND CP.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' 
		AND CPO.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' 
		AND CPO.claim_party_role_code = 'CLMT'
		AND CO.crrnt_snpsht_flag = 1
		AND CP.crrnt_snpsht_flag = 1
		AND CPO.crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claimant_num,claim_party_role_code ORDER BY claim_party_occurrence_ak_id) = 1
),
LKP_Comments_Stage_Coverage_Class_Desc AS (
	SELECT
	TCC_COMMENT_TXT,
	FOLDER_KEY,
	COMMENT_ITEM_NBR
	FROM (
		SELECT A.TCC_COMMENT_TXT AS TCC_COMMENT_TXT,  Rtrim(Ltrim(A.FOLDER_KEY)) AS FOLDER_KEY,  
		A.COMMENT_ITEM_NBR AS COMMENT_ITEM_NBR 
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.CLM_COMMENTS_STAGE A  
		WHERE 
		A.FOLDER_KEY in (SELECT DISTINCT B.COB_CLAIM_NBR FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.CLAIM_OBJECT_STAGE B)
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY FOLDER_KEY,COMMENT_ITEM_NBR ORDER BY TCC_COMMENT_TXT) = 1
),
LKP_Property_Loss_Stage AS (
	SELECT
	CPR_CLAIM_NBR,
	CPR_DAMAGE_DES_ID,
	CPR_OBJECT_TYPE_CD,
	CPR_OBJECT_SEQ_NBR
	FROM (
		SELECT 
		Rtrim(Ltrim(a.CPR_CLAIM_NBR)) as CPR_CLAIM_NBR
		, a.CPR_DAMAGE_DES_ID as CPR_DAMAGE_DES_ID
		, a.CPR_CLAIM_NBR as CPR_CLAIM_NBR
		, Rtrim(Ltrim(a.CPR_OBJECT_TYPE_CD)) as CPR_OBJECT_TYPE_CD
		, a.CPR_OBJECT_SEQ_NBR as CPR_OBJECT_SEQ_NBR 
		FROM 
		@{pipeline().parameters.SOURCE_TABLE_OWNER}.PROPERTY_LOSS_STAGE a
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY CPR_CLAIM_NBR,CPR_OBJECT_TYPE_CD,CPR_OBJECT_SEQ_NBR ORDER BY CPR_CLAIM_NBR) = 1
),
LKP_Comments_Stage_Unit_Damage_Description AS (
	SELECT
	TCC_COMMENT_TXT,
	FOLDER_KEY,
	COMMENT_ITEM_NBR
	FROM (
		SELECT A.TCC_COMMENT_TXT AS TCC_COMMENT_TXT,  Rtrim(Ltrim(A.FOLDER_KEY)) AS FOLDER_KEY,  
		A.COMMENT_ITEM_NBR AS COMMENT_ITEM_NBR 
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.CLM_COMMENTS_STAGE A  
		WHERE 
		A.FOLDER_KEY in (SELECT DISTINCT B.COB_CLAIM_NBR FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.CLAIM_OBJECT_STAGE B)
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY FOLDER_KEY,COMMENT_ITEM_NBR ORDER BY TCC_COMMENT_TXT) = 1
),
EXP_Values AS (
	SELECT
	LKP_Claim_Party_Occurrence_AK_ID.claim_party_occurrence_ak_id,
	-- *INF*: IIF(ISNULL(claim_party_occurrence_ak_id),-1,claim_party_occurrence_ak_id)
	IFF(claim_party_occurrence_ak_id IS NULL, - 1, claim_party_occurrence_ak_id) AS v_claim_party_occurrence_ak_id,
	JNR_coverage_transaction.out_OBJECT_TYPE_CD AS COB_OBJECT_TYPE_CD,
	JNR_coverage_transaction.out_OBJECT_SEQ_NBR AS COB_OBJECT_SEQ_NBR,
	JNR_coverage_transaction.out_PKG_TYPE_CD AS MAJOR_PERIL_CODE,
	JNR_coverage_transaction.out_PKG_SEQ_NBR AS s3p_PKG_SEQ_NUM,
	JNR_coverage_transaction.out_CCP_INS_LINE_CD AS CCP_INS_LINE_CD,
	JNR_coverage_transaction.COB_UNIT_TYPE_CD,
	JNR_coverage_transaction.COB_SAR_ID,
	JNR_coverage_transaction.CCP_SAR_ID,
	LKP_Claim_Party_Occurrence_AK_ID.offset_onset_ind AS claim_occurrence_type_code,
	JNR_coverage_transaction.out_CCP_MNL_ENTRY_IND AS CCP_MNL_ENTRY_IND,
	-- *INF*: iif(CCP_MNL_ENTRY_IND = '1' or claim_occurrence_type_code = 'COM'
	-- 	,ltrim(rtrim(CCP_SAR_ID))
	--       ,ltrim(rtrim(COB_SAR_ID)))
	IFF(
	    CCP_MNL_ENTRY_IND = '1' or claim_occurrence_type_code = 'COM', ltrim(rtrim(CCP_SAR_ID)),
	    ltrim(rtrim(COB_SAR_ID))
	) AS SAR_ID,
	JNR_coverage_transaction.COB_LOC_UNIT_NUM,
	JNR_coverage_transaction.CCP_LOC_UNIT_NUM,
	JNR_coverage_transaction.CCP_RISK_UNIT,
	JNR_coverage_transaction.COB_COV_UNIT_NBR,
	-- *INF*: DECODE(claim_occurrence_type_code,
	--                                                                                    'WCC',ltrim(rtrim(COB_LOC_UNIT_NUM)),
	--                                                                                   'COM',ltrim(rtrim(CCP_LOC_UNIT_NUM))
	-- )
	-- --- Changed to above logic on 7/20/2010 when we found old logic was incorrect.
	-- 
	-- ---------------------------
	-- ---IIF(IN(claim_occurrence_type_code,'AUT','HOM'), ltrim(rtrim(COB_COV_UNIT_NBR)),
	-- -- IIF(claim_occurrence_type_code = 'WCC',
	--    --         IIF(LENGTH(ltrim(rtrim(COB_LOC_UNIT_NUM))) <> 0,ltrim(rtrim(COB_LOC_UNIT_NUM)), ltrim(rtrim(COB_RISK_UNIT))),
	-- ---IIF(claim_occurrence_type_code = 'COM',
	-- --      IIF(CCP_MNL_ENTRY_IND ='1', ltrim(rtrim(COB_COV_UNIT_NBR)),
	--     --          IIF(LENGTH(ltrim(rtrim(CCP_LOC_UNIT_NUM)))<>0, ltrim(rtrim(CCP_LOC_UNIT_NUM)), ltrim(rtrim(CCP_RISK_UNIT)))
	-- --))))
	-- 
	-- 
	DECODE(
	    claim_occurrence_type_code,
	    'WCC', ltrim(rtrim(COB_LOC_UNIT_NUM)),
	    'COM', ltrim(rtrim(CCP_LOC_UNIT_NUM))
	) AS var_LOC_UNIT_NUM,
	-- *INF*: IIF(ISNULL(var_LOC_UNIT_NUM) OR LENGTH(var_LOC_UNIT_NUM) = 0, '0000', ltrim(rtrim(var_LOC_UNIT_NUM)))
	IFF(
	    var_LOC_UNIT_NUM IS NULL OR LENGTH(var_LOC_UNIT_NUM) = 0, '0000',
	    ltrim(rtrim(var_LOC_UNIT_NUM))
	) AS LOC_UNIT_NUM,
	JNR_coverage_transaction.COB_SR_SEQ,
	JNR_coverage_transaction.CCP_MAJR_PERIL_SEQ,
	JNR_coverage_transaction.COB_INS_LINE,
	-- *INF*: IIF(ISNULL(COB_SR_SEQ),
	--                              IIF(ISNULL(CCP_MAJR_PERIL_SEQ),'00',LTRIM(RTRIM(CCP_MAJR_PERIL_SEQ))),
	--            LTRIM(RTRIM(COB_SR_SEQ))
	-- )
	-- 
	-- 
	-- --IIF(CCP_MNL_ENTRY_IND = '1'      ,IIF(COB_INS_LINE = 'WC'   ,ltrim(rtrim(COB_SR_SEQ))  ,ltrim(rtrim(CCP_MAJR_PERIL_SEQ)))      ,IIF(claim_occurrence_type_code = 'COM'             , ltrim(rtrim(CCP_MAJR_PERIL_SEQ))
	-- --             ,ltrim(rtrim(COB_SR_SEQ))))
	IFF(
	    COB_SR_SEQ IS NULL,
	    IFF(
	        CCP_MAJR_PERIL_SEQ IS NULL, '00', LTRIM(RTRIM(CCP_MAJR_PERIL_SEQ))
	    ),
	    LTRIM(RTRIM(COB_SR_SEQ))
	) AS MAJOR_PERIL_SEQ_NUM,
	JNR_coverage_transaction.CCP_INS_LINE,
	-- *INF*: IIF(CCP_MNL_ENTRY_IND = '1'
	--       ,ltrim(rtrim(COB_INS_LINE))
	--       ,IIF(claim_occurrence_type_code = 'COM'
	--              ,ltrim(rtrim(CCP_INS_LINE))
	--              ,ltrim(rtrim(COB_INS_LINE)))
	-- )
	IFF(
	    CCP_MNL_ENTRY_IND = '1', ltrim(rtrim(COB_INS_LINE)),
	    IFF(
	        claim_occurrence_type_code = 'COM', ltrim(rtrim(CCP_INS_LINE)),
	        ltrim(rtrim(COB_INS_LINE))
	    )
	) AS INS_LINE,
	JNR_coverage_transaction.COB_SUB_LOC_NUM,
	JNR_coverage_transaction.CCP_SUB_LOC_NUM,
	-- *INF*: IIF(CCP_MNL_ENTRY_IND = '1'
	--       ,ltrim(rtrim(COB_SUB_LOC_NUM))
	--       ,IIF(claim_occurrence_type_code = 'COM'
	--              ,ltrim(rtrim(CCP_SUB_LOC_NUM))
	--              ,ltrim(rtrim(COB_SUB_LOC_NUM)))
	-- )
	IFF(
	    CCP_MNL_ENTRY_IND = '1', ltrim(rtrim(COB_SUB_LOC_NUM)),
	    IFF(
	        claim_occurrence_type_code = 'COM', ltrim(rtrim(CCP_SUB_LOC_NUM)),
	        ltrim(rtrim(COB_SUB_LOC_NUM))
	    )
	) AS SUB_LOC_UNIT_NUM,
	JNR_coverage_transaction.COB_RISK_UNIT_GRP,
	JNR_coverage_transaction.CCP_RISK_UNIT_GRP,
	-- *INF*: IIF(CCP_MNL_ENTRY_IND = '1'
	--       ,ltrim(rtrim(COB_RISK_UNIT_GRP))
	--       ,IIF(claim_occurrence_type_code = 'COM'
	--              ,ltrim(rtrim(CCP_RISK_UNIT_GRP))
	--              ,ltrim(rtrim(COB_RISK_UNIT_GRP)))
	-- )
	IFF(
	    CCP_MNL_ENTRY_IND = '1', ltrim(rtrim(COB_RISK_UNIT_GRP)),
	    IFF(
	        claim_occurrence_type_code = 'COM', ltrim(rtrim(CCP_RISK_UNIT_GRP)),
	        ltrim(rtrim(COB_RISK_UNIT_GRP))
	    )
	) AS RISK_UNIT_GROUP,
	JNR_coverage_transaction.COB_RSK_UNT_GR_SEQ,
	JNR_coverage_transaction.CCP_RSK_UNT_GR_SEQ,
	-- *INF*: IIF(CCP_MNL_ENTRY_IND = '1'
	--       ,ltrim(rtrim(COB_RSK_UNT_GR_SEQ))
	--       ,IIF(claim_occurrence_type_code = 'COM'
	--              ,ltrim(rtrim(CCP_RSK_UNT_GR_SEQ))
	--              ,ltrim(rtrim(COB_RSK_UNT_GR_SEQ)))
	-- )
	IFF(
	    CCP_MNL_ENTRY_IND = '1', ltrim(rtrim(COB_RSK_UNT_GR_SEQ)),
	    IFF(
	        claim_occurrence_type_code = 'COM', ltrim(rtrim(CCP_RSK_UNT_GR_SEQ)),
	        ltrim(rtrim(COB_RSK_UNT_GR_SEQ))
	    )
	) AS RISK_UNIT_GRP_SEQ,
	JNR_coverage_transaction.COB_RISK_UNIT,
	-- *INF*: DECODE(TRUE,CCP_MNL_ENTRY_IND = '1',ltrim(rtrim(COB_RISK_UNIT)),
	--                                     claim_occurrence_type_code = 'COM', ltrim(rtrim(CCP_RISK_UNIT)),
	--                                     claim_occurrence_type_code = 'AUT',  LTRIM(RTRIM(COB_COV_UNIT_NBR)),
	--                                     claim_occurrence_type_code = 'HOM',  LTRIM(RTRIM(COB_COV_UNIT_NBR)),
	--                                     claim_occurrence_type_code = 'WCC',  LTRIM(RTRIM(COB_RISK_UNIT))
	-- )
	-- 
	-- --- Changed to above logic on 7/20/2010 when we found old logic was incorrect.
	-- 
	DECODE(
	    TRUE,
	    CCP_MNL_ENTRY_IND = '1', ltrim(rtrim(COB_RISK_UNIT)),
	    claim_occurrence_type_code = 'COM', ltrim(rtrim(CCP_RISK_UNIT)),
	    claim_occurrence_type_code = 'AUT', LTRIM(RTRIM(COB_COV_UNIT_NBR)),
	    claim_occurrence_type_code = 'HOM', LTRIM(RTRIM(COB_COV_UNIT_NBR)),
	    claim_occurrence_type_code = 'WCC', LTRIM(RTRIM(COB_RISK_UNIT))
	) AS RISK_UNIT,
	JNR_coverage_transaction.COB_SEQ_RISK_UNIT,
	JNR_coverage_transaction.CCP_SEQ_RISK_UNIT,
	-- *INF*: IIF(CCP_MNL_ENTRY_IND = '1' ,ltrim(rtrim(COB_SEQ_RISK_UNIT)) ,
	-- IIF(claim_occurrence_type_code = 'COM' ,ltrim(rtrim(CCP_SEQ_RISK_UNIT)) ,ltrim(rtrim(COB_SEQ_RISK_UNIT))) )
	-- 
	-- 
	-- 
	-- ---DECODE(TRUE, CCP_MNL_ENTRY_IND = '1' ,ltrim(rtrim(COB_SEQ_RISK_UNIT)) ,
	--                                 ---   claim_occurrence_type_code = 'COM' ,ltrim(rtrim(CCP_SEQ_RISK_UNIT)),
	--                                --     claim_occurrence_type_code  <> 'COM', ltrim(rtrim(COB_SEQ_RISK_UNIT)),
	--                                 --    '0') 
	IFF(
	    CCP_MNL_ENTRY_IND = '1', ltrim(rtrim(COB_SEQ_RISK_UNIT)),
	    IFF(
	        claim_occurrence_type_code = 'COM', ltrim(rtrim(CCP_SEQ_RISK_UNIT)),
	        ltrim(rtrim(COB_SEQ_RISK_UNIT))
	    )
	) AS V_RISK_UNIT_SEQ,
	-- *INF*: IIF(ISNULL(V_RISK_UNIT_SEQ) OR IS_SPACES(V_RISK_UNIT_SEQ) OR LENGTH(V_RISK_UNIT_SEQ)= 0 ,'0',V_RISK_UNIT_SEQ)
	IFF(
	    V_RISK_UNIT_SEQ IS NULL
	    or LENGTH(V_RISK_UNIT_SEQ)>0
	    and TRIM(V_RISK_UNIT_SEQ)=''
	    or LENGTH(V_RISK_UNIT_SEQ) = 0,
	    '0',
	    V_RISK_UNIT_SEQ
	) AS RISK_UNIT_SEQ,
	JNR_coverage_transaction.COB_RISK_TYPE_IND,
	JNR_coverage_transaction.CCP_RISK_TYPE_IND,
	-- *INF*:  IIF(CCP_MNL_ENTRY_IND = '1'
	--       ,ltrim(rtrim(COB_RISK_TYPE_IND))
	--       ,IIF(claim_occurrence_type_code = 'COM'
	--              ,ltrim(rtrim(CCP_RISK_TYPE_IND))
	--              ,ltrim(rtrim(COB_RISK_TYPE_IND)))
	-- )
	IFF(
	    CCP_MNL_ENTRY_IND = '1', ltrim(rtrim(COB_RISK_TYPE_IND)),
	    IFF(
	        claim_occurrence_type_code = 'COM', ltrim(rtrim(CCP_RISK_TYPE_IND)),
	        ltrim(rtrim(COB_RISK_TYPE_IND))
	    )
	) AS RISK_TYPE_IND,
	LKP_Comments_Stage_Coverage_Class_Desc.TCC_COMMENT_TXT AS s3p_WC_CLASS_DESCRIPTION,
	-- *INF*: IIF(ISNULL(s3p_WC_CLASS_DESCRIPTION) OR IS_SPACES(s3p_WC_CLASS_DESCRIPTION)
	-- ,'N/A'
	-- ,ltrim(rtrim(s3p_WC_CLASS_DESCRIPTION)))
	IFF(
	    s3p_WC_CLASS_DESCRIPTION IS NULL
	    or LENGTH(s3p_WC_CLASS_DESCRIPTION)>0
	    and TRIM(s3p_WC_CLASS_DESCRIPTION)='',
	    'N/A',
	    ltrim(rtrim(s3p_WC_CLASS_DESCRIPTION))
	) AS s3p_WC_CLASS_DESCRIPTION_OUT,
	JNR_coverage_transaction.out_SPP_USE_CD AS spec_pers_prop_use_code,
	-- *INF*: IIF(ISNULL(spec_pers_prop_use_code) OR IS_SPACES(spec_pers_prop_use_code)
	-- ,'N/A'
	-- ,ltrim(rtrim(spec_pers_prop_use_code)))
	IFF(
	    spec_pers_prop_use_code IS NULL
	    or LENGTH(spec_pers_prop_use_code)>0
	    and TRIM(spec_pers_prop_use_code)='',
	    'N/A',
	    ltrim(rtrim(spec_pers_prop_use_code))
	) AS spec_pers_prop_use_code_out,
	JNR_coverage_transaction.CCP_PKG_EFF_DT,
	JNR_coverage_transaction.CCP_PKG_EXP_DT,
	JNR_coverage_transaction.CCP_PKG_DED_AMT,
	-- *INF*: IIF(ISNULL(CCP_PKG_DED_AMT) 
	-- ,0
	-- ,CCP_PKG_DED_AMT)
	IFF(CCP_PKG_DED_AMT IS NULL, 0, CCP_PKG_DED_AMT) AS CCP_PKG_DED_AMT_OUT,
	JNR_coverage_transaction.CCP_PKG_LIMIT_AMT,
	-- *INF*: IIF(ISNULL(CCP_PKG_LIMIT_AMT) 
	-- ,0
	-- ,CCP_PKG_LIMIT_AMT)
	IFF(CCP_PKG_LIMIT_AMT IS NULL, 0, CCP_PKG_LIMIT_AMT) AS CCP_PKG_LIMIT_AMT_OUT,
	LKP_Claim_Comments_Stage_Unit_Desc.TCC_COMMENT_TXT AS s3p_UNIT_DESCRIPTION,
	-- *INF*: IIF(ISNULL(s3p_UNIT_DESCRIPTION) OR IS_SPACES(s3p_UNIT_DESCRIPTION)
	-- ,'N/A'
	-- ,ltrim(rtrim(s3p_UNIT_DESCRIPTION)))
	IFF(
	    s3p_UNIT_DESCRIPTION IS NULL
	    or LENGTH(s3p_UNIT_DESCRIPTION)>0
	    and TRIM(s3p_UNIT_DESCRIPTION)='',
	    'N/A',
	    ltrim(rtrim(s3p_UNIT_DESCRIPTION))
	) AS S3P_UNIT_DESCRIPTION_OUT,
	LKP_Auto_Loss_Stage.CAU_CAR_YR,
	-- *INF*: IIF(ISNULL(CAU_CAR_YR)
	-- ,0
	-- ,CAU_CAR_YR)
	IFF(CAU_CAR_YR IS NULL, 0, CAU_CAR_YR) AS CAU_CAR_YR_OUT,
	LKP_Auto_Loss_Stage.CAU_CAR_MAKE_NM,
	-- *INF*: IIF(ISNULL(CAU_CAR_MAKE_NM) OR IS_SPACES(CAU_CAR_MAKE_NM)
	-- ,'N/A'
	-- ,ltrim(rtrim(CAU_CAR_MAKE_NM)))
	IFF(
	    CAU_CAR_MAKE_NM IS NULL OR LENGTH(CAU_CAR_MAKE_NM)>0 AND TRIM(CAU_CAR_MAKE_NM)='', 'N/A',
	    ltrim(rtrim(CAU_CAR_MAKE_NM))
	) AS CAU_CAR_MAKE_NM_OUT,
	LKP_Auto_Loss_Stage.CAU_CAR_RGS_NBR,
	-- *INF*: IIF(ISNULL(CAU_CAR_RGS_NBR) OR IS_SPACES(CAU_CAR_RGS_NBR)
	-- ,'N/A'
	-- ,ltrim(rtrim(CAU_CAR_RGS_NBR)))
	IFF(
	    CAU_CAR_RGS_NBR IS NULL OR LENGTH(CAU_CAR_RGS_NBR)>0 AND TRIM(CAU_CAR_RGS_NBR)='', 'N/A',
	    ltrim(rtrim(CAU_CAR_RGS_NBR))
	) AS CAU_CAR_RGS_NBR_OUT,
	LKP_Auto_Loss_Stage.CAU_VEHICLE_ID_NBR,
	-- *INF*: IIF(ISNULL(CAU_VEHICLE_ID_NBR) OR IS_SPACES(CAU_VEHICLE_ID_NBR)
	-- ,'N/A'
	-- ,ltrim(rtrim(CAU_VEHICLE_ID_NBR)))
	IFF(
	    CAU_VEHICLE_ID_NBR IS NULL OR LENGTH(CAU_VEHICLE_ID_NBR)>0 AND TRIM(CAU_VEHICLE_ID_NBR)='',
	    'N/A',
	    ltrim(rtrim(CAU_VEHICLE_ID_NBR))
	) AS CAU_VEHICLE_ID_NBR_OUT,
	LKP_Auto_Loss_Stage.CAU_RGS_STATE_CD,
	-- *INF*: IIF(ISNULL(CAU_RGS_STATE_CD) OR IS_SPACES(CAU_RGS_STATE_CD)
	-- ,'N/A'
	-- ,ltrim(rtrim(CAU_RGS_STATE_CD)))
	IFF(
	    CAU_RGS_STATE_CD IS NULL OR LENGTH(CAU_RGS_STATE_CD)>0 AND TRIM(CAU_RGS_STATE_CD)='', 'N/A',
	    ltrim(rtrim(CAU_RGS_STATE_CD))
	) AS CAU_RGS_STATE_CD_OUT,
	LKP_Auto_Loss_Stage.CAU_VEH_ST_AMT,
	-- *INF*: IIF(ISNULL(CAU_VEH_ST_AMT)
	-- ,0
	-- ,CAU_VEH_ST_AMT)
	IFF(CAU_VEH_ST_AMT IS NULL, 0, CAU_VEH_ST_AMT) AS CAU_VEH_ST_AMT_OUT,
	LKP_Comments_Stage_Unit_Damage_Description.TCC_COMMENT_TXT AS UNIT_DAMAGE_DESCRIPTION,
	-- *INF*: IIF(ISNULL(UNIT_DAMAGE_DESCRIPTION) OR IS_SPACES(UNIT_DAMAGE_DESCRIPTION)
	-- ,'N/A'
	-- ,UNIT_DAMAGE_DESCRIPTION)
	IFF(
	    UNIT_DAMAGE_DESCRIPTION IS NULL
	    or LENGTH(UNIT_DAMAGE_DESCRIPTION)>0
	    and TRIM(UNIT_DAMAGE_DESCRIPTION)='',
	    'N/A',
	    UNIT_DAMAGE_DESCRIPTION
	) AS UNIT_DAMAGE_DESCRIPTION_OUT,
	JNR_coverage_transaction.ctx_bur_cause_loss,
	-- *INF*: IIF(ISNULL(ctx_bur_cause_loss) OR IS_SPACES(ctx_bur_cause_loss), 'N/A', SUBSTR(ctx_bur_cause_loss, 1,2))
	IFF(
	    ctx_bur_cause_loss IS NULL OR LENGTH(ctx_bur_cause_loss)>0 AND TRIM(ctx_bur_cause_loss)='',
	    'N/A',
	    SUBSTR(ctx_bur_cause_loss, 1, 2)
	) AS cause_of_loss_out,
	-- *INF*: IIF(ISNULL(ctx_bur_cause_loss) OR IS_SPACES(ctx_bur_cause_loss), 'N/A', SUBSTR(ctx_bur_cause_loss, 3,1))
	-- 
	IFF(
	    ctx_bur_cause_loss IS NULL OR LENGTH(ctx_bur_cause_loss)>0 AND TRIM(ctx_bur_cause_loss)='',
	    'N/A',
	    SUBSTR(ctx_bur_cause_loss, 3, 1)
	) AS reserve_ctgry_out,
	JNR_coverage_transaction.ccp_coverage_form,
	JNR_coverage_transaction.cob_coverage_form,
	-- *INF*:  IIF(CCP_MNL_ENTRY_IND = '1'
	--       ,ltrim(rtrim(cob_coverage_form))
	--       ,IIF(claim_occurrence_type_code = 'COM'
	--              ,ltrim(rtrim(ccp_coverage_form))
	--              ,ltrim(rtrim(cob_coverage_form)))
	-- )
	IFF(
	    CCP_MNL_ENTRY_IND = '1', ltrim(rtrim(cob_coverage_form)),
	    IFF(
	        claim_occurrence_type_code = 'COM', ltrim(rtrim(ccp_coverage_form)),
	        ltrim(rtrim(cob_coverage_form))
	    )
	) AS o_CoverageForm,
	JNR_coverage_transaction.ccp_coverage_type,
	JNR_coverage_transaction.cob_coverage_type,
	-- *INF*:  IIF(CCP_MNL_ENTRY_IND = '1'
	--       ,ltrim(rtrim(cob_coverage_type))
	--       ,IIF(claim_occurrence_type_code = 'COM'
	--              ,ltrim(rtrim(ccp_coverage_type))
	--              ,ltrim(rtrim(cob_coverage_type)))
	-- )
	IFF(
	    CCP_MNL_ENTRY_IND = '1', ltrim(rtrim(cob_coverage_type)),
	    IFF(
	        claim_occurrence_type_code = 'COM', ltrim(rtrim(ccp_coverage_type)),
	        ltrim(rtrim(cob_coverage_type))
	    )
	) AS o_Coveragetype,
	JNR_coverage_transaction.ccp_risk_type,
	JNR_coverage_transaction.cob_risk_type,
	-- *INF*:  IIF(CCP_MNL_ENTRY_IND = '1'
	--       ,ltrim(rtrim(cob_risk_type))
	--       ,IIF(claim_occurrence_type_code = 'COM'
	--              ,ltrim(rtrim(ccp_risk_type))
	--              ,ltrim(rtrim(cob_risk_type)))
	-- )
	IFF(
	    CCP_MNL_ENTRY_IND = '1', ltrim(rtrim(cob_risk_type)),
	    IFF(
	        claim_occurrence_type_code = 'COM', ltrim(rtrim(ccp_risk_type)),
	        ltrim(rtrim(cob_risk_type))
	    )
	) AS o_RiskType,
	JNR_coverage_transaction.cvr_policy_src_id,
	JNR_coverage_transaction.ccp_pol_cov_id AS i_ccp_pol_cov_id,
	-- *INF*: LTRIM(RTRIM(i_ccp_pol_cov_id))
	LTRIM(RTRIM(i_ccp_pol_cov_id)) AS ccp_pol_cov_id
	FROM JNR_coverage_transaction
	LEFT JOIN LKP_Auto_Loss_Stage
	ON LKP_Auto_Loss_Stage.CAU_CLAIM_NBR = JNR_coverage_transaction.COB_CLAIM_NBR AND LKP_Auto_Loss_Stage.CAU_OBJECT_SEQ_NBR = JNR_coverage_transaction.out_OBJECT_SEQ_NBR
	LEFT JOIN LKP_Claim_Comments_Stage_Unit_Desc
	ON LKP_Claim_Comments_Stage_Unit_Desc.FOLDER_KEY = JNR_coverage_transaction.COB_CLAIM_NBR AND LKP_Claim_Comments_Stage_Unit_Desc.COMMENT_ITEM_NBR = JNR_coverage_transaction.COB_UNIT_DES_ID
	LEFT JOIN LKP_Claim_Party_Occurrence_AK_ID
	ON LKP_Claim_Party_Occurrence_AK_ID.claimant_num = JNR_coverage_transaction.COB_CLAIM_NBR AND LKP_Claim_Party_Occurrence_AK_ID.claim_party_role_code = EXP_Client_ID.out_CLIENT_ID
	LEFT JOIN LKP_Comments_Stage_Coverage_Class_Desc
	ON LKP_Comments_Stage_Coverage_Class_Desc.FOLDER_KEY = JNR_coverage_transaction.COB_CLAIM_NBR AND LKP_Comments_Stage_Coverage_Class_Desc.COMMENT_ITEM_NBR = JNR_coverage_transaction.COB_OBJECT_CMT_ID
	LEFT JOIN LKP_Comments_Stage_Unit_Damage_Description
	ON LKP_Comments_Stage_Unit_Damage_Description.FOLDER_KEY = LKP_Property_Loss_Stage.CPR_CLAIM_NBR AND LKP_Comments_Stage_Unit_Damage_Description.COMMENT_ITEM_NBR = LKP_Property_Loss_Stage.CPR_DAMAGE_DES_ID
),
LKP_claim_party_occurrence AS (
	SELECT
	claim_occurrence_ak_id,
	claim_party_occurrence_ak_id
	FROM (
		SELECT 
			claim_occurrence_ak_id,
			claim_party_occurrence_ak_id
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_party_occurrence
		WHERE source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' and crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_party_occurrence_ak_id ORDER BY claim_occurrence_ak_id) = 1
),
LKP_claim_occurrence AS (
	SELECT
	pol_key_ak_id,
	cvr_policy_src_id,
	claim_occurrence_ak_id
	FROM (
		SELECT 
			pol_key_ak_id,
			cvr_policy_src_id,
			claim_occurrence_ak_id
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_occurrence
		WHERE source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' and crrnt_snpsht_flag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_occurrence_ak_id ORDER BY pol_key_ak_id DESC) = 1
),
LKP_Policy AS (
	SELECT
	pms_pol_lob_code,
	InsuranceSegmentAKId,
	PolicyOfferingCode,
	pol_ak_id
	FROM (
		SELECT P.pms_pol_lob_code as pms_pol_lob_code, 
			P.InsuranceSegmentAKId as InsuranceSegmentAKId, 
			po.PolicyOfferingCode as PolicyOfferingCode,
			P.pol_ak_id as pol_ak_id 
		FROM V2.policy P 
		JOIN dbo.PolicyOffering po ON po.PolicyOfferingAKId = p.PolicyOfferingAKId and po.CurrentSnapshotFlag = 1
		WHERE P.pol_ak_id IN (SELECT DISTINCT pol_key_ak_id 
				from @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_occurrence CO)
			AND P.crrnt_snpsht_flag =1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_ak_id ORDER BY pms_pol_lob_code) = 1
),
LKP_RatingCoverage AS (
	SELECT
	CoverageVersion,
	AnnualStatementLineNumber,
	ClassCode,
	SublineCode,
	RatingCoverageAKID,
	ProductAKId,
	InsuranceReferenceLineOfBusinessAKId,
	PolicyAkid,
	TypeBureauCode,
	CoverageGUID
	FROM (
		SELECT RC.CoverageGUID as CoverageGUID,
		RC.CoverageVersion as CoverageVersion,
		RC.AnnualStatementLineNumber as AnnualStatementLineNumber,
		RC.ClassCode as ClassCode, 
		RC .SublineCode as SublineCode,
		RC.RatingCoverageAKID as RatingCoverageAKID, 
		RC.ProductAKId as ProductAKId, 
		RC.InsuranceReferenceLineOfBusinessAKId as InsuranceReferenceLineOfBusinessAKId, 
		PC.PolicyAkid as PolicyAkid,
		PC.TypeBureauCode as TypeBureauCode 
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyCoverage PC 	
		INNER JOIN @{pipeline().parameters.TARGET_TABLE_OWNER}.RatingCoverage RC 
		ON PC.PolicyCoverageAKID=RC.PolicyCoverageAKID AND PC.CurrentSnapshotFlag=1 AND PC.SourceSystemID='DCT'
		WHERE PC.PolicyAKID IN (SELECT DISTINCT pol_key_ak_id from @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_occurrence CO 
		INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.clm_cov_pkg_stage S ON CO.claim_occurrence_key = S.ccp_claim_nbr )
		Order by PC.PolicyAkid,RC.CoverageGUID,RC.Effectivedate,RC.CreatedDate
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyAkid,CoverageGUID ORDER BY CoverageVersion DESC) = 1
),
LKP_Product AS (
	SELECT
	LOBCode,
	ProductAKId
	FROM (
		SELECT CASE WHEN ProductCode  = '000' THEN 'N/A'
		            WHEN ProductCode = '100' THEN 'WCP' 
		            WHEN ProductCode in ('610','620','630','640','650','660') THEN 'BND'
		            WHEN ProductCode in ('800','850','890') THEN 'HAP'
		            WHEN ProductCode in ('200','300','400','410','420','430','450','500','510','550','520','320','900','') THEN 'CPP'
		            ELSE 'CPP' END AS LOBCode, 
		Product.ProductAKId as ProductAKId FROM Product
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ProductAKId ORDER BY LOBCode DESC) = 1
),
EXP_LOBCode AS (
	SELECT
	LKP_Product.LOBCode,
	LKP_Policy.pms_pol_lob_code,
	LKP_Policy.PolicyOfferingCode,
	-- *INF*: DECODE(TRUE,
	-- 	PolicyOfferingCode = '801',
	-- 		'CHO',
	-- 	pms_pol_lob_code = 'N/A', 
	-- 		LOBCode,
	-- 	pms_pol_lob_code)
	-- 
	-- -- Policy Offering '801' is PL Choice, so LOB = 'CHO'
	-- ---For PMS policies, pms_pol_lob_code is never 'N/A'
	DECODE(
	    TRUE,
	    PolicyOfferingCode = '801', 'CHO',
	    pms_pol_lob_code = 'N/A', LOBCode,
	    pms_pol_lob_code
	) AS LOBCode_Out
	FROM 
	LEFT JOIN LKP_Policy
	ON LKP_Policy.pol_ak_id = LKP_claim_occurrence.pol_key_ak_id
	LEFT JOIN LKP_Product
	ON LKP_Product.ProductAKId = LKP_RatingCoverage.ProductAKId
),
LKP_sup_CauseOfLoss AS (
	SELECT
	CauseOfLossId,
	LineOfBusiness,
	MajorPeril,
	CauseOfLoss
	FROM (
		SELECT LTRIM(RTRIM(a.MajorPeril)) as MajorPeril,
		a.LineOfBusiness as LineOfBusiness,
		a.CauseOfLoss as CauseOfLoss,,
		a.CauseOfLossId as CauseOfLossId
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_CauseOfLoss a
		where a.CurrentSnapshotFlag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY LineOfBusiness,MajorPeril,CauseOfLoss ORDER BY CauseOfLossId) = 1
),
EXP_Get_CauseOfLossID AS (
	SELECT
	CauseOfLossId,
	-- *INF*: IIF(ISNULL(CauseOfLossId), -1, CauseOfLossId)
	IFF(CauseOfLossId IS NULL, - 1, CauseOfLossId) AS CauseOfLossId_out
	FROM LKP_sup_CauseOfLoss
),
LKP_sup_insurance_line_code AS (
	SELECT
	sup_ins_line_id,
	StandardInsuranceLineCode,
	IN_ins_line_code,
	ins_line_code
	FROM (
		SELECT a.sup_ins_line_id as sup_ins_line_id,
		a.StandardInsuranceLineCode as StandardInsuranceLineCode,
		 LTRIM(RTRIM(a.ins_line_code)) as ins_line_code
		FROM 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.sup_insurance_line a
		where crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ins_line_code ORDER BY sup_ins_line_id) = 1
),
EXP_Get_Ins_Line AS (
	SELECT
	LKP_sup_insurance_line_code.sup_ins_line_id AS i_sup_ins_line_id_code,
	LKP_sup_insurance_line_code.StandardInsuranceLineCode AS i_StandardInsuranceLineCode_ins_line_code,
	LKP_sup_insurance_line_code.IN_ins_line_code AS i_ins_line_code,
	EXP_Values.cvr_policy_src_id,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(i_StandardInsuranceLineCode_ins_line_code) AND ISNULL(i_ins_line_code), 'N/A',
	-- ISNULL(i_StandardInsuranceLineCode_ins_line_code) AND cvr_policy_src_id = 'PMS', i_ins_line_code,
	-- ISNULL(i_StandardInsuranceLineCode_ins_line_code), 'N/A',
	-- i_StandardInsuranceLineCode_ins_line_code)
	-- 
	-- -- we only want to fall back on the i_ins_line_code if claim is for a PMS policy, those are the only ones using stat cov.  All others must either be a valid StandardLineInsCode or be set to N/A
	-- 
	-- --IIF(ISNULL(i_StandardInsuranceLineCode_ins_line_code),IIF(ISNULL(i_ins_line_code),'N/A',i_ins_line_code),i_StandardInsuranceLineCode_ins_line_code)
	DECODE(
	    TRUE,
	    i_StandardInsuranceLineCode_ins_line_code IS NULL AND i_ins_line_code IS NULL, 'N/A',
	    i_StandardInsuranceLineCode_ins_line_code IS NULL AND cvr_policy_src_id = 'PMS', i_ins_line_code,
	    i_StandardInsuranceLineCode_ins_line_code IS NULL, 'N/A',
	    i_StandardInsuranceLineCode_ins_line_code
	) AS o_ins_line,
	-- *INF*: IIF(ISNULL(i_sup_ins_line_id_code),-1,i_sup_ins_line_id_code)
	IFF(i_sup_ins_line_id_code IS NULL, - 1, i_sup_ins_line_id_code) AS o_SupInsuranceLineID
	FROM EXP_Values
	LEFT JOIN LKP_sup_insurance_line_code
	ON LKP_sup_insurance_line_code.ins_line_code = EXP_Values.INS_LINE
),
LKP_sup_risk_unit AS (
	SELECT
	IN_risk_unit_code,
	sup_risk_unit_id,
	StandardRiskUnitCode,
	risk_unit_code,
	ins_line
	FROM (
		SELECT LTRIM(RTRIM(a.risk_unit_code)) as risk_unit_code,
		LTRIM(RTRIM(a.ins_line)) as ins_line,
		a.sup_risk_unit_id as sup_risk_unit_id,
		a.StandardRiskUnitCode as StandardRiskUnitCode
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_risk_unit a
		where a.crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY risk_unit_code,ins_line ORDER BY IN_risk_unit_code) = 1
),
EXP_Get_Risk_Unit AS (
	SELECT
	sup_risk_unit_id,
	-- *INF*: IIF(ISNULL(sup_risk_unit_id), -1, sup_risk_unit_id)
	IFF(sup_risk_unit_id IS NULL, - 1, sup_risk_unit_id) AS out_sup_risk_unit_id,
	IN_risk_unit_code AS risk_unit_code,
	StandardRiskUnitCode,
	-- *INF*: IIF(ISNULL(StandardRiskUnitCode),IIF(ISNULL(risk_unit_code),'N/A',risk_unit_code),StandardRiskUnitCode)
	IFF(
	    StandardRiskUnitCode IS NULL,
	    IFF(
	        risk_unit_code IS NULL, 'N/A', risk_unit_code
	    ),
	    StandardRiskUnitCode
	) AS out_StandardRiskUnitCode
	FROM LKP_sup_risk_unit
),
LKP_sup_risk_unit_group_code AS (
	SELECT
	IN_risk_unit_grp_code,
	sup_risk_unit_grp_id,
	risk_unit_grp_code,
	ins_line,
	prdct_type_code
	FROM (
		SELECT a.risk_unit_grp_code as risk_unit_grp_code,
		a.ins_line as ins_line,
		LTRIM(RTRIM(a.prdct_type_code)) as prdct_type_code,
		a.sup_risk_unit_grp_id as sup_risk_unit_grp_id
		FROM 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.sup_risk_unit_group a
		where crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY risk_unit_grp_code,ins_line,prdct_type_code ORDER BY IN_risk_unit_grp_code) = 1
),
EXP_Get_Risk_Unit_Grp_Id AS (
	SELECT
	IN_risk_unit_grp_code AS risk_unit_grp_code,
	risk_unit_grp_code AS lkp_risk_unit_grp_code,
	-- *INF*: IIF(ISNULL(lkp_risk_unit_grp_code),IIF(ISNULL(risk_unit_grp_code),'N/A',risk_unit_grp_code),lkp_risk_unit_grp_code)
	IFF(
	    lkp_risk_unit_grp_code IS NULL,
	    IFF(
	        risk_unit_grp_code IS NULL, 'N/A', risk_unit_grp_code
	    ),
	    lkp_risk_unit_grp_code
	) AS risk_unit_grp,
	sup_risk_unit_grp_id AS sup_risk_unit_grp_id_code,
	-- *INF*: IIF(ISNULL(sup_risk_unit_grp_id_code),-1,sup_risk_unit_grp_id_code)
	IFF(sup_risk_unit_grp_id_code IS NULL, - 1, sup_risk_unit_grp_id_code) AS sup_risk_unit_grp_id
	FROM LKP_sup_risk_unit_group_code
),
LKP_PolicyCoverage_PMS AS (
	SELECT
	TypeBureauCode,
	cvr_policy_src_id,
	PolicyAKID
	FROM (
		SELECT PC.TypeBureauCode as TypeBureauCode, 
		PC.PolicyAKID as PolicyAKID 
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyCoverage PC
		WHERE PC.policyakid IN (SELECT DISTINCT pol_key_ak_id from @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_occurrence CO 
		INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.clm_cov_pkg_stage S ON CO.claim_occurrence_key = S.ccp_claim_nbr )
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyAKID ORDER BY TypeBureauCode) = 1
),
EXP_Get_TypBureauCode AS (
	SELECT
	LKP_PolicyCoverage_PMS.TypeBureauCode AS TypeBureauCode_PMS,
	LKP_RatingCoverage.TypeBureauCode AS TypeBureauCode_DCT,
	-- *INF*: IIF(ltrim(rtrim(cvr_policy_src_id))='DUC', IIF(ISNULL(TypeBureauCode_DCT), 'N/A', TypeBureauCode_DCT), 'N/A')
	-- 
	-- --We will be only evaluating DuckCreek records and the PMS evaluation is done in a different mapping which updates the Type Bureau code from N/A to the correct value.
	IFF(
	    ltrim(rtrim(cvr_policy_src_id)) = 'DUC',
	    IFF(
	        TypeBureauCode_DCT IS NULL, 'N/A', TypeBureauCode_DCT
	    ),
	    'N/A'
	) AS TypeBureauCode,
	LKP_PolicyCoverage_PMS.cvr_policy_src_id
	FROM 
	LEFT JOIN LKP_PolicyCoverage_PMS
	ON LKP_PolicyCoverage_PMS.PolicyAKID = LKP_claim_occurrence.pol_key_ak_id
	LEFT JOIN LKP_RatingCoverage
	ON LKP_RatingCoverage.PolicyAkid = LKP_claim_occurrence.pol_key_ak_id AND LKP_RatingCoverage.CoverageGUID = EXP_Values.ccp_pol_cov_id
),
LKP_InsuranceSegment_PMS AS (
	SELECT
	InsuranceSegmentCode,
	InsuranceSegmentAKId
	FROM (
		SELECT 
			InsuranceSegmentCode,
			InsuranceSegmentAKId
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.InsuranceSegment
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY InsuranceSegmentAKId ORDER BY InsuranceSegmentCode DESC) = 1
),
LKP_claimant_coverage_detail AS (
	SELECT
	claimant_cov_det_ak_id,
	claim_party_occurrence_ak_id,
	s3p_object_type_code,
	s3p_object_seq_num,
	s3p_pkg_seq_num,
	s3p_ins_line_code,
	s3p_unit_type_code,
	s3p_wc_class_descript,
	loc_unit_num,
	sub_loc_unit_num,
	ins_line,
	risk_unit_grp,
	risk_unit_grp_seq_num,
	risk_unit,
	risk_unit_seq_num,
	major_peril_code,
	major_peril_seq,
	reserve_ctgry,
	cause_of_loss,
	claimant_cov_eff_date,
	claimant_cov_exp_date,
	risk_type_ind,
	s3p_unit_descript,
	spec_pers_prop_use_code,
	pkg_ded_amt,
	pkg_lmt_amt,
	manual_entry_ind,
	unit_veh_registration_state_code,
	unit_veh_stated_amt,
	unit_dam_descript,
	unit_veh_yr,
	unit_veh_make,
	unit_vin_num,
	CoverageGUID,
	pms_type_bureau_code,
	IN_claim_party_occurrence_ak_id,
	IN_COB_OBJECT_TYPE_CD,
	IN_COB_OBJECT_SEQ_NBR,
	IN_MAJOR_PERIL_CODE1,
	IN_s3p_PKG_SEQ_NUM1,
	IN_cause_of_loss_out,
	IN_reserve_ctgry_out
	FROM (
		SELECT a.claimant_cov_det_ak_id as claimant_cov_det_ak_id,
		LTRIM(RTRIM(a.s3p_ins_line_code)) as s3p_ins_line_code, 
		LTRIM(RTRIM(a.s3p_unit_type_code)) as s3p_unit_type_code, 
		LTRIM(RTRIM(a.s3p_wc_class_descript)) as s3p_wc_class_descript, 
		LTRIM(RTRIM(a.loc_unit_num)) as loc_unit_num, 
		LTRIM(RTRIM(a.sub_loc_unit_num)) as sub_loc_unit_num, 
		LTRIM(RTRIM(a.ins_line)) as ins_line, 
		LTRIM(RTRIM(a.risk_unit_grp)) as risk_unit_grp, 
		LTRIM(RTRIM(a.risk_unit_grp_seq_num)) as risk_unit_grp_seq_num, 
		LTRIM(RTRIM(a.risk_unit)) as risk_unit, 
		LTRIM(RTRIM(a.risk_unit_seq_num)) as risk_unit_seq_num, 
		LTRIM(RTRIM(a.major_peril_seq)) as major_peril_seq, 
		a.claimant_cov_eff_date as claimant_cov_eff_date, 
		a.claimant_cov_exp_date as claimant_cov_exp_date, 
		LTRIM(RTRIM(a.risk_type_ind)) as risk_type_ind, 
		LTRIM(RTRIM(a.s3p_unit_descript)) as s3p_unit_descript, 
		LTRIM(RTRIM(a.spec_pers_prop_use_code)) as spec_pers_prop_use_code, 
		a.pkg_ded_amt as pkg_ded_amt, 
		a.pkg_lmt_amt as pkg_lmt_amt, 
		LTRIM(RTRIM(a.manual_entry_ind)) as manual_entry_ind,
		LTRIM(RTRIM(a.unit_veh_registration_state_code)) as unit_veh_registration_state_code, 
		a.unit_veh_stated_amt as unit_veh_stated_amt, 
		LTRIM(RTRIM(a.unit_dam_descript)) as unit_dam_descript, 
		a.unit_veh_yr as unit_veh_yr, 
		LTRIM(RTRIM(a.unit_veh_make)) as unit_veh_make, 
		LTRIM(RTRIM(a.unit_vin_num)) as unit_vin_num, 
		a.claim_party_occurrence_ak_id as claim_party_occurrence_ak_id, 
		LTRIM(RTRIM(a.s3p_object_type_code)) as s3p_object_type_code, 
		a.s3p_object_seq_num as s3p_object_seq_num, 
		LTRIM(RTRIM(a.major_peril_code)) as major_peril_code, 
		a.s3p_pkg_seq_num as s3p_pkg_seq_num, 
		a.cause_of_loss as cause_of_loss, 
		a.reserve_ctgry as reserve_ctgry,
		a.CoverageGUID as CoverageGUID, 
		LTRIM(RTRIM(a.pms_type_bureau_code)) as pms_type_bureau_code
		FROM 
		@{pipeline().parameters.TARGET_TABLE_OWNER}.claimant_coverage_detail a
		where a.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
		and a.crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_party_occurrence_ak_id,s3p_object_type_code,s3p_object_seq_num,major_peril_code,s3p_pkg_seq_num,cause_of_loss,reserve_ctgry ORDER BY claimant_cov_det_ak_id) = 1
),
LKP_sup_major_peril AS (
	SELECT
	sup_major_peril_id,
	major_peril_code
	FROM (
		SELECT LTRIM(RTRIM(a.major_peril_code)) as major_peril_code,
		a.sup_major_peril_id as sup_major_peril_id
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_major_peril a
		where a.crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY major_peril_code ORDER BY sup_major_peril_id) = 1
),
LKP_sup_state AS (
	SELECT
	sup_state_id,
	state_code
	FROM (
		SELECT LTRIM(RTRIM(a.state_code)) as state_code,
		a.sup_state_id as sup_state_id
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_state a
		where a.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}' and a.crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY state_code ORDER BY sup_state_id) = 1
),
LKP_sup_type_bureau_code AS (
	SELECT
	sup_type_bureau_code_id,
	type_bureau_code
	FROM (
		SELECT 
			sup_type_bureau_code_id,
			type_bureau_code
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_type_bureau_code
		WHERE crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY type_bureau_code ORDER BY sup_type_bureau_code_id) = 1
),
EXP_Detect_Changes AS (
	SELECT
	LKP_claimant_coverage_detail.s3p_ins_line_code AS old_s3p_ins_line_code,
	LKP_claimant_coverage_detail.s3p_unit_type_code AS old_s3p_unit_type_code,
	LKP_claimant_coverage_detail.s3p_wc_class_descript AS old_s3p_wc_class_descript,
	LKP_claimant_coverage_detail.loc_unit_num AS old_loc_unit_num,
	LKP_claimant_coverage_detail.sub_loc_unit_num AS old_sub_loc_unit_num,
	LKP_claimant_coverage_detail.ins_line AS old_ins_line,
	LKP_claimant_coverage_detail.risk_unit_grp AS old_risk_unit_grp,
	LKP_claimant_coverage_detail.risk_unit_grp_seq_num AS old_risk_unit_grp_seq_num,
	LKP_claimant_coverage_detail.risk_unit AS old_risk_unit,
	LKP_claimant_coverage_detail.risk_unit_seq_num AS old_risk_unit_seq_num,
	LKP_claimant_coverage_detail.major_peril_seq AS old_major_peril_seq,
	LKP_claimant_coverage_detail.claimant_cov_eff_date AS old_claimant_cov_eff_date,
	LKP_claimant_coverage_detail.claimant_cov_exp_date AS old_claimant_cov_exp_date,
	LKP_claimant_coverage_detail.risk_type_ind AS old_risk_type_ind,
	LKP_claimant_coverage_detail.s3p_unit_descript AS old_s3p_unit_descript,
	LKP_claimant_coverage_detail.spec_pers_prop_use_code AS old_spec_pers_prop_use_code,
	LKP_claimant_coverage_detail.pkg_ded_amt AS old_pkg_ded_amt,
	LKP_claimant_coverage_detail.pkg_lmt_amt AS old_pkg_lmt_amt,
	LKP_claimant_coverage_detail.manual_entry_ind AS old_manual_entry_ind,
	LKP_claimant_coverage_detail.unit_veh_registration_state_code AS old_unit_veh_registration_state_code,
	LKP_claimant_coverage_detail.unit_veh_stated_amt AS old_unit_veh_stated_amt,
	LKP_claimant_coverage_detail.unit_dam_descript AS old_unit_dam_descript,
	LKP_claimant_coverage_detail.unit_veh_yr AS old_unit_veh_yr,
	LKP_claimant_coverage_detail.unit_veh_make AS old_unit_veh_make,
	LKP_claimant_coverage_detail.unit_vin_num AS old_unit_vin_num,
	LKP_claimant_coverage_detail.CoverageGUID AS old_CoverageGUID,
	LKP_claimant_coverage_detail.pms_type_bureau_code AS old_pms_type_bureau_code,
	EXP_Values.ccp_pol_cov_id,
	LKP_sup_major_peril.sup_major_peril_id AS SupMajorPerilID,
	EXP_Values.o_CoverageForm AS coverage_form,
	EXP_Values.o_Coveragetype AS coverage_type,
	EXP_Values.o_RiskType AS risk_type,
	LKP_sup_state.sup_state_id,
	LKP_sup_type_bureau_code.sup_type_bureau_code_id,
	LKP_RatingCoverage.CoverageVersion,
	LKP_RatingCoverage.AnnualStatementLineNumber,
	LKP_RatingCoverage.ClassCode,
	LKP_RatingCoverage.SublineCode,
	LKP_RatingCoverage.RatingCoverageAKID,
	LKP_claimant_coverage_detail.claimant_cov_det_ak_id,
	LKP_claimant_coverage_detail.IN_claim_party_occurrence_ak_id AS claim_party_occurrence_ak_id,
	EXP_Values.COB_OBJECT_TYPE_CD,
	EXP_Values.COB_OBJECT_SEQ_NBR,
	EXP_Values.MAJOR_PERIL_CODE,
	EXP_Values.s3p_PKG_SEQ_NUM,
	EXP_Values.CCP_INS_LINE_CD,
	EXP_Values.COB_UNIT_TYPE_CD,
	EXP_Values.LOC_UNIT_NUM,
	EXP_Values.MAJOR_PERIL_SEQ_NUM,
	EXP_Get_Ins_Line.o_ins_line AS INS_LINE,
	EXP_Values.SUB_LOC_UNIT_NUM,
	EXP_Get_Risk_Unit_Grp_Id.risk_unit_grp AS RISK_UNIT_GROUP,
	EXP_Values.RISK_UNIT_GRP_SEQ,
	EXP_Get_Risk_Unit.out_StandardRiskUnitCode AS RISK_UNIT,
	EXP_Values.RISK_UNIT_SEQ,
	EXP_Values.RISK_TYPE_IND,
	EXP_Values.s3p_WC_CLASS_DESCRIPTION_OUT AS s3p_WC_CLASS_DESCRIPTION,
	EXP_Values.spec_pers_prop_use_code_out AS spec_pers_prop_use_code,
	EXP_Values.CCP_PKG_EFF_DT,
	EXP_Values.CCP_PKG_EXP_DT,
	EXP_Values.CCP_PKG_DED_AMT_OUT AS CCP_PKG_DED_AMT,
	EXP_Values.CCP_PKG_LIMIT_AMT_OUT AS CCP_PKG_LIMIT_AMT,
	EXP_Values.S3P_UNIT_DESCRIPTION_OUT AS s3p_UNIT_DESCRIPTION,
	EXP_Values.CAU_CAR_YR_OUT AS CAU_CAR_YR,
	EXP_Values.CAU_CAR_MAKE_NM_OUT AS CAU_CAR_MAKE_NM,
	EXP_Values.CAU_CAR_RGS_NBR_OUT AS CAU_CAR_RGS_NBR,
	EXP_Values.CAU_VEHICLE_ID_NBR_OUT AS CAU_VEHICLE_ID_NBR,
	EXP_Values.CAU_RGS_STATE_CD_OUT AS CAU_RGS_STATE_CD,
	EXP_Values.CAU_VEH_ST_AMT_OUT AS CAU_VEH_ST_AMT,
	EXP_Values.UNIT_DAMAGE_DESCRIPTION_OUT AS UNIT_DAMAGE_DESCRIPTION,
	EXP_Values.CCP_MNL_ENTRY_IND,
	EXP_Values.cause_of_loss_out,
	EXP_Values.reserve_ctgry_out,
	EXP_Get_Ins_Line.o_SupInsuranceLineID AS SupInsuranceLineID,
	EXP_Get_Risk_Unit_Grp_Id.sup_risk_unit_grp_id,
	EXP_Get_Risk_Unit.out_sup_risk_unit_id AS sup_risk_unit_id,
	EXP_Get_CauseOfLossID.CauseOfLossId_out,
	EXP_Values.cvr_policy_src_id,
	EXP_Get_TypBureauCode.TypeBureauCode,
	LKP_claim_occurrence.pol_key_ak_id,
	LKP_RatingCoverage.ProductAKId AS ProductAKId_DCT,
	LKP_RatingCoverage.InsuranceReferenceLineOfBusinessAKId AS InsuranceReferenceLineOfBusinessAKId_DCT,
	-- *INF*: IIF(ISNULL(ccp_pol_cov_id), 'N/A', ccp_pol_cov_id)
	IFF(ccp_pol_cov_id IS NULL, 'N/A', ccp_pol_cov_id) AS v_ccp_pol_cov_id,
	-- *INF*: iif(isnull(claimant_cov_det_ak_id)
	-- , 'NEW'
	-- ,iif(ltrim(rtrim(old_s3p_ins_line_code))  != ltrim(rtrim(CCP_INS_LINE_CD)) or 
	-- ltrim(rtrim(old_s3p_unit_type_code))  != ltrim(rtrim(COB_UNIT_TYPE_CD)) or
	-- ltrim(rtrim(old_s3p_wc_class_descript))  != ltrim(rtrim(s3p_WC_CLASS_DESCRIPTION)) or
	-- ltrim(rtrim(old_loc_unit_num))  !=  ltrim(rtrim(LOC_UNIT_NUM)) or
	-- ltrim(rtrim(old_sub_loc_unit_num))  != ltrim(rtrim(SUB_LOC_UNIT_NUM)) or
	-- ltrim(rtrim(old_ins_line))  != ltrim(rtrim(INS_LINE)) or
	-- ltrim(rtrim(old_risk_unit_grp))  != ltrim(rtrim(RISK_UNIT_GROUP)) or 
	-- ltrim(rtrim(old_risk_unit_grp_seq_num))  != ltrim(rtrim(RISK_UNIT_GRP_SEQ)) or
	-- ltrim(rtrim(old_risk_unit)) != ltrim(rtrim(RISK_UNIT)) or
	-- ltrim(rtrim(old_risk_unit_seq_num))  != ltrim(rtrim(RISK_UNIT_SEQ)) or
	-- ltrim(rtrim(old_major_peril_seq)) != ltrim(rtrim(MAJOR_PERIL_SEQ_NUM)) or
	-- old_claimant_cov_eff_date != CCP_PKG_EFF_DT or
	-- old_claimant_cov_exp_date != CCP_PKG_EXP_DT or
	-- ltrim(rtrim(old_risk_type_ind)) != ltrim(rtrim(RISK_TYPE_IND))  OR 
	-- ltrim(rtrim(old_s3p_unit_descript)) != ltrim(rtrim(s3p_UNIT_DESCRIPTION))  OR 
	-- ltrim(rtrim(old_spec_pers_prop_use_code)) != ltrim(rtrim(spec_pers_prop_use_code)) OR 
	-- old_pkg_ded_amt != CCP_PKG_DED_AMT OR 
	-- old_pkg_lmt_amt != CCP_PKG_LIMIT_AMT OR 
	-- ltrim(rtrim(old_manual_entry_ind)) != ltrim(rtrim(CCP_MNL_ENTRY_IND)) or
	-- ltrim(rtrim(old_unit_veh_registration_state_code)) != ltrim(rtrim(CAU_RGS_STATE_CD)) OR 
	-- old_unit_veh_stated_amt != CAU_VEH_ST_AMT OR 
	-- ltrim(rtrim(old_unit_dam_descript)) != ltrim(rtrim(UNIT_DAMAGE_DESCRIPTION)) OR 
	-- old_unit_veh_yr != CAU_CAR_YR OR 
	-- ltrim(rtrim(old_unit_veh_make)) != ltrim(rtrim(CAU_CAR_MAKE_NM)) OR 
	-- ltrim(rtrim(old_unit_vin_num)) != ltrim(rtrim(CAU_VEHICLE_ID_NBR)) OR
	-- (cvr_policy_src_id='DUC' AND old_CoverageGUID<>v_ccp_pol_cov_id)
	-- ,'UPDATE'
	-- ,'NOCHANGE')
	-- )
	IFF(
	    claimant_cov_det_ak_id IS NULL, 'NEW',
	    IFF(
	        ltrim(rtrim(old_s3p_ins_line_code)) != ltrim(rtrim(CCP_INS_LINE_CD))
	        or ltrim(rtrim(old_s3p_unit_type_code)) != ltrim(rtrim(COB_UNIT_TYPE_CD))
	        or ltrim(rtrim(old_s3p_wc_class_descript)) != ltrim(rtrim(s3p_WC_CLASS_DESCRIPTION))
	        or ltrim(rtrim(old_loc_unit_num)) != ltrim(rtrim(LOC_UNIT_NUM))
	        or ltrim(rtrim(old_sub_loc_unit_num)) != ltrim(rtrim(SUB_LOC_UNIT_NUM))
	        or ltrim(rtrim(old_ins_line)) != ltrim(rtrim(INS_LINE))
	        or ltrim(rtrim(old_risk_unit_grp)) != ltrim(rtrim(RISK_UNIT_GROUP))
	        or ltrim(rtrim(old_risk_unit_grp_seq_num)) != ltrim(rtrim(RISK_UNIT_GRP_SEQ))
	        or ltrim(rtrim(old_risk_unit)) != ltrim(rtrim(RISK_UNIT))
	        or ltrim(rtrim(old_risk_unit_seq_num)) != ltrim(rtrim(RISK_UNIT_SEQ))
	        or ltrim(rtrim(old_major_peril_seq)) != ltrim(rtrim(MAJOR_PERIL_SEQ_NUM))
	        or old_claimant_cov_eff_date != CCP_PKG_EFF_DT
	        or old_claimant_cov_exp_date != CCP_PKG_EXP_DT
	        or ltrim(rtrim(old_risk_type_ind)) != ltrim(rtrim(RISK_TYPE_IND))
	        or ltrim(rtrim(old_s3p_unit_descript)) != ltrim(rtrim(s3p_UNIT_DESCRIPTION))
	        or ltrim(rtrim(old_spec_pers_prop_use_code)) != ltrim(rtrim(spec_pers_prop_use_code))
	        or old_pkg_ded_amt != CCP_PKG_DED_AMT
	        or old_pkg_lmt_amt != CCP_PKG_LIMIT_AMT
	        or ltrim(rtrim(old_manual_entry_ind)) != ltrim(rtrim(CCP_MNL_ENTRY_IND))
	        or ltrim(rtrim(old_unit_veh_registration_state_code)) != ltrim(rtrim(CAU_RGS_STATE_CD))
	        or old_unit_veh_stated_amt != CAU_VEH_ST_AMT
	        or ltrim(rtrim(old_unit_dam_descript)) != ltrim(rtrim(UNIT_DAMAGE_DESCRIPTION))
	        or old_unit_veh_yr != CAU_CAR_YR
	        or ltrim(rtrim(old_unit_veh_make)) != ltrim(rtrim(CAU_CAR_MAKE_NM))
	        or ltrim(rtrim(old_unit_vin_num)) != ltrim(rtrim(CAU_VEHICLE_ID_NBR))
	        or (cvr_policy_src_id = 'DUC'
	        and old_CoverageGUID <> v_ccp_pol_cov_id),
	        'UPDATE',
	        'NOCHANGE'
	    )
	) AS v_Changed_Flag,
	1 AS Crrnt_SnapSht_Flag,
	0 AS logical_flag,
	v_Changed_Flag AS changed_flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AUDIT_ID,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS SOURCE_SYSTEM_ID,
	-- *INF*: iif(v_Changed_Flag='NEW',
	-- 	to_date('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'),sysdate)
	IFF(
	    v_Changed_Flag = 'NEW', TO_TIMESTAMP('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'),
	    CURRENT_TIMESTAMP
	) AS eff_from_date,
	-- *INF*: to_date('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_TIMESTAMP('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS eff_to_date,
	sysdate AS created_date,
	-- *INF*: IIF(ISNULL(SupMajorPerilID), -1, SupMajorPerilID)
	IFF(SupMajorPerilID IS NULL, - 1, SupMajorPerilID) AS out_SupMajorPerilID,
	-- *INF*: IIF(ISNULL(coverage_form), 'N/A', coverage_form)
	IFF(coverage_form IS NULL, 'N/A', coverage_form) AS coverage_form_out,
	-- *INF*: IIF(ISNULL(coverage_type), 'N/A', coverage_type)
	IFF(coverage_type IS NULL, 'N/A', coverage_type) AS coverage_type_out,
	-- *INF*: IIF(ISNULL(risk_type), 'N/A', risk_type)
	IFF(risk_type IS NULL, 'N/A', risk_type) AS risk_type_out,
	-- *INF*: IIF(ISNULL(sup_state_id), -1, sup_state_id)
	IFF(sup_state_id IS NULL, - 1, sup_state_id) AS sup_state_id_out,
	-- *INF*: IIF(ISNULL(sup_type_bureau_code_id), -1, sup_type_bureau_code_id)
	IFF(sup_type_bureau_code_id IS NULL, - 1, sup_type_bureau_code_id) AS sup_type_bureau_code_id_out,
	-- *INF*: IIF(ISNULL(CoverageVersion), 'N/A', CoverageVersion)
	IFF(CoverageVersion IS NULL, 'N/A', CoverageVersion) AS CoverageVersion_out,
	-- *INF*: IIF(ISNULL(AnnualStatementLineNumber), 'N/A', AnnualStatementLineNumber)
	IFF(AnnualStatementLineNumber IS NULL, 'N/A', AnnualStatementLineNumber) AS AnnualStatementLineNumber_out,
	-- *INF*: IIF(ISNULL(ClassCode), 'N/A', ClassCode)
	IFF(ClassCode IS NULL, 'N/A', ClassCode) AS ClassCode_out,
	-- *INF*: IIF(ISNULL(SublineCode), 'N/A', SublineCode)
	IFF(SublineCode IS NULL, 'N/A', SublineCode) AS SublineCode_out,
	-- *INF*: IIF(ISNULL(RatingCoverageAKID), -1, RatingCoverageAKID)
	IFF(RatingCoverageAKID IS NULL, - 1, RatingCoverageAKID) AS RatingCoverageAKID_out,
	v_ccp_pol_cov_id AS ccp_pol_cov_id_out,
	-- *INF*: DECODE(TRUE,
	-- RISK_UNIT_SEQ='0' AND INS_LINE='WC',
	-- '00',
	-- IN(RISK_UNIT_SEQ, '0','1') AND INS_LINE<>'WC' AND RISK_TYPE_IND='N/A',
	-- 'N/A',
	-- IN(RISK_UNIT_SEQ, '0','1','2','3','4','8') AND INS_LINE='GL',
	-- RISK_UNIT_SEQ || RISK_TYPE_IND,
	-- RISK_UNIT_SEQ
	-- )
	DECODE(
	    TRUE,
	    RISK_UNIT_SEQ = '0' AND INS_LINE = 'WC', '00',
	    RISK_UNIT_SEQ IN ('0','1') AND INS_LINE <> 'WC' AND RISK_TYPE_IND = 'N/A', 'N/A',
	    RISK_UNIT_SEQ IN ('0','1','2','3','4','8') AND INS_LINE = 'GL', RISK_UNIT_SEQ || RISK_TYPE_IND,
	    RISK_UNIT_SEQ
	) AS o_RiskUnitSequenceNumber_AKId,
	'N/A' AS o_pms_type_bureau_code,
	LKP_InsuranceSegment_PMS.InsuranceSegmentCode AS i_InsuranceSegmentCode,
	-- *INF*: IIF(ISNULL(i_InsuranceSegmentCode),'N/A',i_InsuranceSegmentCode)
	IFF(i_InsuranceSegmentCode IS NULL, 'N/A', i_InsuranceSegmentCode) AS o_InsuranceSegmentCode
	FROM EXP_Get_CauseOfLossID
	 -- Manually join with EXP_Get_Ins_Line
	 -- Manually join with EXP_Get_Risk_Unit
	 -- Manually join with EXP_Get_Risk_Unit_Grp_Id
	 -- Manually join with EXP_Get_TypBureauCode
	 -- Manually join with EXP_Values
	LEFT JOIN LKP_InsuranceSegment_PMS
	ON LKP_InsuranceSegment_PMS.InsuranceSegmentAKId = LKP_Policy.InsuranceSegmentAKId
	LEFT JOIN LKP_RatingCoverage
	ON LKP_RatingCoverage.PolicyAkid = LKP_claim_occurrence.pol_key_ak_id AND LKP_RatingCoverage.CoverageGUID = EXP_Values.ccp_pol_cov_id
	LEFT JOIN LKP_claim_occurrence
	ON LKP_claim_occurrence.claim_occurrence_ak_id = LKP_claim_party_occurrence.claim_occurrence_ak_id
	LEFT JOIN LKP_claimant_coverage_detail
	ON LKP_claimant_coverage_detail.claim_party_occurrence_ak_id = EXP_Values.v_claim_party_occurrence_ak_id AND LKP_claimant_coverage_detail.s3p_object_type_code = EXP_Values.COB_OBJECT_TYPE_CD AND LKP_claimant_coverage_detail.s3p_object_seq_num = EXP_Values.COB_OBJECT_SEQ_NBR AND LKP_claimant_coverage_detail.major_peril_code = EXP_Values.MAJOR_PERIL_CODE AND LKP_claimant_coverage_detail.s3p_pkg_seq_num = EXP_Values.s3p_PKG_SEQ_NUM AND LKP_claimant_coverage_detail.cause_of_loss = EXP_Values.cause_of_loss_out AND LKP_claimant_coverage_detail.reserve_ctgry = EXP_Values.reserve_ctgry_out
	LEFT JOIN LKP_sup_major_peril
	ON LKP_sup_major_peril.major_peril_code = EXP_Values.MAJOR_PERIL_CODE
	LEFT JOIN LKP_sup_state
	ON LKP_sup_state.state_code = EXP_Values.CAU_RGS_STATE_CD_OUT
	LEFT JOIN LKP_sup_type_bureau_code
	ON LKP_sup_type_bureau_code.type_bureau_code = EXP_Get_TypBureauCode.TypeBureauCode
),
FIL_Insert AS (
	SELECT
	claimant_cov_det_ak_id, 
	claim_party_occurrence_ak_id, 
	COB_OBJECT_TYPE_CD, 
	COB_OBJECT_SEQ_NBR, 
	MAJOR_PERIL_CODE, 
	s3p_PKG_SEQ_NUM, 
	CCP_INS_LINE_CD, 
	COB_UNIT_TYPE_CD, 
	LOC_UNIT_NUM, 
	MAJOR_PERIL_SEQ_NUM, 
	INS_LINE, 
	SUB_LOC_UNIT_NUM, 
	RISK_UNIT_GROUP, 
	RISK_UNIT_GRP_SEQ, 
	RISK_UNIT, 
	RISK_UNIT_SEQ, 
	RISK_TYPE_IND, 
	s3p_WC_CLASS_DESCRIPTION, 
	spec_pers_prop_use_code, 
	CCP_PKG_EFF_DT, 
	CCP_PKG_EXP_DT, 
	CCP_PKG_DED_AMT, 
	CCP_PKG_LIMIT_AMT, 
	s3p_UNIT_DESCRIPTION, 
	CAU_CAR_YR, 
	CAU_CAR_MAKE_NM, 
	CAU_CAR_RGS_NBR, 
	CAU_VEHICLE_ID_NBR, 
	CAU_RGS_STATE_CD, 
	CAU_VEH_ST_AMT, 
	UNIT_DAMAGE_DESCRIPTION, 
	CCP_MNL_ENTRY_IND, 
	Crrnt_SnapSht_Flag, 
	logical_flag, 
	changed_flag, 
	AUDIT_ID, 
	SOURCE_SYSTEM_ID, 
	eff_from_date, 
	eff_to_date, 
	created_date, 
	cause_of_loss_out, 
	reserve_ctgry_out, 
	SupInsuranceLineID, 
	sup_risk_unit_grp_id, 
	sup_risk_unit_id, 
	out_SupMajorPerilID AS SupMajorPerilID, 
	coverage_form_out AS coverage_form, 
	coverage_type_out AS coverage_type, 
	risk_type_out AS risk_type, 
	CauseOfLossId_out, 
	cvr_policy_src_id, 
	sup_state_id_out AS sup_state_id, 
	TypeBureauCode, 
	sup_type_bureau_code_id_out AS sup_type_bureau_code_id, 
	CoverageVersion_out, 
	AnnualStatementLineNumber_out, 
	ClassCode_out, 
	SublineCode_out, 
	RatingCoverageAKID_out, 
	ccp_pol_cov_id_out, 
	pol_key_ak_id, 
	ProductAKId_DCT, 
	InsuranceReferenceLineOfBusinessAKId_DCT, 
	o_RiskUnitSequenceNumber_AKId AS RiskUnitSequenceNumber_AKId, 
	o_pms_type_bureau_code AS pms_type_bureau_code, 
	o_InsuranceSegmentCode
	FROM EXP_Detect_Changes
	WHERE changed_flag='NEW' or changed_flag='UPDATE'
),
LKP_StatisticalCoverageForPMSExceed AS (
	SELECT
	InsuranceReferenceLineOfBusinessAKId,
	ProductAKId,
	StatisticalCoverageAKID,
	CoverageGuid,
	PolicyAKID,
	InsuranceLine,
	LocationNumber,
	MajorPerilCode,
	MajorPerilSequenceNumber,
	RiskUnit,
	RiskUnitSequenceNumber,
	RiskUnitGroup,
	RiskUnitGroupSequenceNumber,
	SubLocationUnitNumber,
	TypeBureauCode,
	MaxPolicyCovEffDate
	FROM (
		Select 
		DISTINCT SC.InsuranceReferenceLineOfBusinessAKID as InsuranceReferenceLineOfBusinessAKID,
		SC.ProductAKID as ProductAKID,
		SC.StatisticalCoverageAKID as StatisticalCoverageAKID,
		SC.CoverageGuid as CoverageGuid,
		PC.PolicyAKID as PolicyAKID, 
		PC.InsuranceLine as InsuranceLine, 
		(CASE WHEN RL.LocationIndicator = 'N' THEN '0000' ELSE RL.LocationUnitNumber END) as LocationNumber,
		SC.MajorPerilCode as MajorPerilCode,
		SC.MajorPerilSequenceNumber as MajorPerilSequenceNumber,
		SC.RiskUnit as RiskUnit,
		(CASE WHEN SC.RiskUnitSequenceNumber = '0' then 'N/A' ELSE SC.RiskUnitSequenceNumber END) as RiskUnitSequenceNumber,
		SC.RiskUnitGroup as RiskUnitGroup,
		SC.RiskUnitGroupSequenceNumber as RiskUnitGroupSequenceNumber,
		(CASE WHEN SC.SubLocationUnitNumber = 'N/A' THEN '000' ELSE SC.SubLocationUnitNumber END) as SubLocationUnitNumber,
		PC.TypeBureauCode as TypeBureauCode,
		MAX(PC.PolicyCoverageEffectiveDate) as MaxPolicyCovEffDate
		
		FROM
		@{pipeline().parameters.TARGET_TABLE_OWNER}.StatisticalCoverage SC ,
		@{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyCoverage PC,
		@{pipeline().parameters.TARGET_TABLE_OWNER}.RiskLocation RL,
		V2.policy p
		WHERE SC.PolicyCoverageAKID = PC.PolicyCoverageAKID 
		AND PC.RiskLocationAKID = RL.RiskLocationAKID  
		AND  PC.PolicyAKID = p.pol_ak_id 
		AND P.crrnt_snpsht_flag=1 
		AND P.source_sys_id='PMS'
		AND  EXISTS (SELECT DISTINCT pol_key_ak_id 
		from @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_occurrence,
		@{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PIF_42X6_STAGE
		where claim_occurrence_key=(pif_symbol+pif_policy_number+pif_module+right('0'+convert(varchar,ipfcx6_month_of_loss),2) +right('0'+convert(varchar,ipfcx6_day_of_loss),2)+convert(varchar,ipfcx6_year_of_loss)+ipfcx6_loss_occ_fdigit+right('0'+convert(varchar,ipfcx6_usr_loss_occurence),2) ) and 
		crrnt_snpsht_flag = 1 AND PC.PolicyAKID= pol_key_ak_id )
		GROUP BY SC.InsuranceReferenceLineOfBusinessAKID,
		SC.ProductAKID,
		SC.StatisticalCoverageAKID,
		SC.CoverageGuid,
		PC.PolicyAKID, 
		PC.InsuranceLine,
		CASE WHEN RL.LocationIndicator = 'N' THEN '0000' ELSE RL.LocationUnitNumber END,
		SC.MajorPerilCode,
		SC.MajorPerilSequenceNumber,
		SC.RiskUnit,
		CASE WHEN SC.RiskUnitSequenceNumber = '0' then 'N/A' ELSE SC.RiskUnitSequenceNumber END ,
		SC.RiskUnitGroup, 
		SC.RiskUnitGroupSequenceNumber ,
		CASE WHEN SC.SubLocationUnitNumber = 'N/A' THEN '000' ELSE SC.SubLocationUnitNumber END ,
		PC.TypeBureauCode
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyAKID,InsuranceLine,LocationNumber,MajorPerilCode,MajorPerilSequenceNumber,RiskUnit,RiskUnitSequenceNumber,RiskUnitGroup,RiskUnitGroupSequenceNumber,SubLocationUnitNumber,TypeBureauCode,MaxPolicyCovEffDate ORDER BY InsuranceReferenceLineOfBusinessAKId) = 1
),
LKP_SupTypeOfLossRules AS (
	SELECT
	TypeOfLoss,
	ClaimTypeCategory,
	ClaimTypeGroup,
	SubrogationEligibleIndicator,
	MajorPerilCode,
	CauseOfLoss,
	InsuranceSegmentCode
	FROM (
		SELECT 
			TypeOfLoss,
			ClaimTypeCategory,
			ClaimTypeGroup,
			SubrogationEligibleIndicator,
			MajorPerilCode,
			CauseOfLoss,
			InsuranceSegmentCode
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.SupTypeOfLossRules
		WHERE CurrentSnapshotFlag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY MajorPerilCode,CauseOfLoss,InsuranceSegmentCode ORDER BY TypeOfLoss DESC) = 1
),
SEQ_Claimant_Coverage_Detail AS (
	CREATE SEQUENCE SEQ_Claimant_Coverage_Detail
	START = 0
	INCREMENT = 1;
),
EXP_Determine_AK AS (
	SELECT
	FIL_Insert.claimant_cov_det_ak_id,
	SEQ_Claimant_Coverage_Detail.NEXTVAL,
	-- *INF*: iif(isnull(claimant_cov_det_ak_id)
	-- ,NEXTVAL
	-- ,claimant_cov_det_ak_id)
	IFF(claimant_cov_det_ak_id IS NULL, NEXTVAL, claimant_cov_det_ak_id) AS out_claimant_cov_det_ak_id,
	FIL_Insert.claim_party_occurrence_ak_id,
	FIL_Insert.COB_OBJECT_TYPE_CD,
	FIL_Insert.COB_OBJECT_SEQ_NBR,
	FIL_Insert.MAJOR_PERIL_CODE,
	FIL_Insert.s3p_PKG_SEQ_NUM,
	FIL_Insert.CCP_INS_LINE_CD,
	FIL_Insert.COB_UNIT_TYPE_CD,
	FIL_Insert.LOC_UNIT_NUM,
	FIL_Insert.MAJOR_PERIL_SEQ_NUM,
	FIL_Insert.INS_LINE,
	FIL_Insert.SUB_LOC_UNIT_NUM,
	FIL_Insert.RISK_UNIT_GROUP,
	FIL_Insert.RISK_UNIT_GRP_SEQ,
	FIL_Insert.RISK_UNIT,
	FIL_Insert.RISK_UNIT_SEQ,
	FIL_Insert.RISK_TYPE_IND,
	FIL_Insert.s3p_WC_CLASS_DESCRIPTION,
	FIL_Insert.spec_pers_prop_use_code,
	FIL_Insert.CCP_PKG_EFF_DT,
	FIL_Insert.CCP_PKG_EXP_DT,
	FIL_Insert.CCP_PKG_DED_AMT,
	FIL_Insert.CCP_PKG_LIMIT_AMT,
	FIL_Insert.s3p_UNIT_DESCRIPTION,
	FIL_Insert.CAU_CAR_YR,
	FIL_Insert.CAU_CAR_MAKE_NM,
	FIL_Insert.CAU_CAR_RGS_NBR,
	FIL_Insert.CAU_VEHICLE_ID_NBR,
	FIL_Insert.CAU_RGS_STATE_CD,
	FIL_Insert.CAU_VEH_ST_AMT,
	FIL_Insert.UNIT_DAMAGE_DESCRIPTION,
	FIL_Insert.CCP_MNL_ENTRY_IND,
	FIL_Insert.Crrnt_SnapSht_Flag,
	FIL_Insert.logical_flag,
	FIL_Insert.changed_flag,
	FIL_Insert.AUDIT_ID,
	FIL_Insert.SOURCE_SYSTEM_ID,
	FIL_Insert.eff_from_date,
	FIL_Insert.eff_to_date,
	FIL_Insert.created_date,
	'N/A' AS dummy,
	FIL_Insert.cause_of_loss_out,
	FIL_Insert.reserve_ctgry_out,
	FIL_Insert.SupInsuranceLineID,
	FIL_Insert.sup_risk_unit_grp_id,
	FIL_Insert.sup_risk_unit_id,
	FIL_Insert.SupMajorPerilID,
	FIL_Insert.coverage_form,
	FIL_Insert.coverage_type,
	FIL_Insert.risk_type,
	FIL_Insert.CauseOfLossId_out,
	FIL_Insert.cvr_policy_src_id,
	FIL_Insert.sup_state_id,
	FIL_Insert.TypeBureauCode,
	FIL_Insert.sup_type_bureau_code_id,
	FIL_Insert.CoverageVersion_out,
	FIL_Insert.AnnualStatementLineNumber_out,
	FIL_Insert.ClassCode_out,
	FIL_Insert.SublineCode_out,
	FIL_Insert.RatingCoverageAKID_out,
	FIL_Insert.ccp_pol_cov_id_out AS ccp_pol_cov_id_in,
	-- *INF*: IIF(ISNULL(i_CoverageGuid_PMS),'N/A',i_CoverageGuid_PMS)
	IFF(i_CoverageGuid_PMS IS NULL, 'N/A', i_CoverageGuid_PMS) AS v_CoverageGuid_PMS,
	-- *INF*: IIF(IN(cvr_policy_src_id,'DUC','PDC'),ccp_pol_cov_id_in,
	-- v_CoverageGuid_PMS)
	IFF(cvr_policy_src_id IN ('DUC','PDC'), ccp_pol_cov_id_in, v_CoverageGuid_PMS) AS ccp_pol_cov_id_out,
	LKP_StatisticalCoverageForPMSExceed.InsuranceReferenceLineOfBusinessAKId AS i_InsuranceReferenceLineOfBusinessAKId_PMS,
	LKP_StatisticalCoverageForPMSExceed.ProductAKId AS i_ProductAKId_PMS,
	LKP_StatisticalCoverageForPMSExceed.StatisticalCoverageAKID AS i_StatisticalCoverageAKID_PMS,
	LKP_StatisticalCoverageForPMSExceed.CoverageGuid AS i_CoverageGuid_PMS,
	FIL_Insert.ProductAKId_DCT AS i_ProductAKId_DCT,
	FIL_Insert.InsuranceReferenceLineOfBusinessAKId_DCT AS i_InsuranceReferenceLineOfBusinessAKId_DCT,
	-- *INF*: DECODE(TRUE,
	-- cvr_policy_src_id='PMS'  AND  NOT ISNULL(i_InsuranceReferenceLineOfBusinessAKId_PMS),
	-- i_InsuranceReferenceLineOfBusinessAKId_PMS,
	-- cvr_policy_src_id='PMS'  AND ISNULL(i_InsuranceReferenceLineOfBusinessAKId_PMS),
	-- -1,
	-- IN(cvr_policy_src_id,'DUC','PDC') AND  NOT ISNULL(i_InsuranceReferenceLineOfBusinessAKId_DCT),
	-- i_InsuranceReferenceLineOfBusinessAKId_DCT,
	-- -1)
	DECODE(
	    TRUE,
	    cvr_policy_src_id = 'PMS' AND i_InsuranceReferenceLineOfBusinessAKId_PMS IS NOT NULL, i_InsuranceReferenceLineOfBusinessAKId_PMS,
	    cvr_policy_src_id = 'PMS' AND i_InsuranceReferenceLineOfBusinessAKId_PMS IS NULL, - 1,
	    cvr_policy_src_id IN ('DUC','PDC') AND i_InsuranceReferenceLineOfBusinessAKId_DCT IS NOT NULL, i_InsuranceReferenceLineOfBusinessAKId_DCT,
	    - 1
	) AS o_InsuranceReferenceLineOfBusinessAKId,
	-- *INF*: DECODE(TRUE,
	-- cvr_policy_src_id='PMS'  AND  NOT ISNULL(i_ProductAKId_PMS),
	-- i_ProductAKId_PMS,
	-- cvr_policy_src_id='PMS'  AND ISNULL(i_ProductAKId_PMS),
	-- -1,
	-- IN(cvr_policy_src_id,'DUC','PDC') AND  NOT ISNULL(i_ProductAKId_DCT),
	-- i_ProductAKId_DCT,
	-- -1)
	DECODE(
	    TRUE,
	    cvr_policy_src_id = 'PMS' AND i_ProductAKId_PMS IS NOT NULL, i_ProductAKId_PMS,
	    cvr_policy_src_id = 'PMS' AND i_ProductAKId_PMS IS NULL, - 1,
	    cvr_policy_src_id IN ('DUC','PDC') AND i_ProductAKId_DCT IS NOT NULL, i_ProductAKId_DCT,
	    - 1
	) AS o_ProductAKId,
	-- *INF*: DECODE(TRUE,
	-- cvr_policy_src_id='PMS'  AND  NOT ISNULL(i_StatisticalCoverageAKID_PMS),
	-- i_StatisticalCoverageAKID_PMS,
	-- -1)
	DECODE(
	    TRUE,
	    cvr_policy_src_id = 'PMS' AND i_StatisticalCoverageAKID_PMS IS NOT NULL, i_StatisticalCoverageAKID_PMS,
	    - 1
	) AS o_StatisticalCoverageAKId,
	LKP_SupTypeOfLossRules.TypeOfLoss AS i_TypeOfLoss,
	-- *INF*: IIF(ISNULL(i_TypeOfLoss) ,'N/A',i_TypeOfLoss)
	-- 
	-- --IIF(ISNULL(i_TypeOfLoss)  OR i_TypeOfLoss = 'Unassigned'   ,'N/A',i_TypeOfLoss)
	IFF(i_TypeOfLoss IS NULL, 'N/A', i_TypeOfLoss) AS o_TypeOfLoss,
	LKP_SupTypeOfLossRules.ClaimTypeCategory AS i_ClaimTypeCategory,
	-- *INF*: IIF(ISNULL(i_ClaimTypeCategory) ,'N/A',i_ClaimTypeCategory)
	IFF(i_ClaimTypeCategory IS NULL, 'N/A', i_ClaimTypeCategory) AS o_ClaimTypeCategory,
	LKP_SupTypeOfLossRules.ClaimTypeGroup AS i_ClaimTypeGroup,
	-- *INF*: IIF(ISNULL(i_ClaimTypeGroup),'N/A',i_ClaimTypeGroup)
	IFF(i_ClaimTypeGroup IS NULL, 'N/A', i_ClaimTypeGroup) AS o_ClaimTypeGroup,
	LKP_SupTypeOfLossRules.SubrogationEligibleIndicator AS i_SubrogationEligibleIndicator,
	-- *INF*: IIF(ISNULL(i_SubrogationEligibleIndicator),'N/A',i_SubrogationEligibleIndicator)
	IFF(i_SubrogationEligibleIndicator IS NULL, 'N/A', i_SubrogationEligibleIndicator) AS o_SubrogationEligibleIndicator
	FROM FIL_Insert
	LEFT JOIN LKP_StatisticalCoverageForPMSExceed
	ON LKP_StatisticalCoverageForPMSExceed.PolicyAKID = FIL_Insert.pol_key_ak_id AND LKP_StatisticalCoverageForPMSExceed.InsuranceLine = FIL_Insert.INS_LINE AND LKP_StatisticalCoverageForPMSExceed.LocationNumber = FIL_Insert.LOC_UNIT_NUM AND LKP_StatisticalCoverageForPMSExceed.MajorPerilCode = FIL_Insert.MAJOR_PERIL_CODE AND LKP_StatisticalCoverageForPMSExceed.MajorPerilSequenceNumber = FIL_Insert.MAJOR_PERIL_SEQ_NUM AND LKP_StatisticalCoverageForPMSExceed.RiskUnit = FIL_Insert.RISK_UNIT AND LKP_StatisticalCoverageForPMSExceed.RiskUnitSequenceNumber = FIL_Insert.RiskUnitSequenceNumber_AKId AND LKP_StatisticalCoverageForPMSExceed.RiskUnitGroup = FIL_Insert.RISK_UNIT_GROUP AND LKP_StatisticalCoverageForPMSExceed.RiskUnitGroupSequenceNumber = FIL_Insert.RISK_UNIT_GRP_SEQ AND LKP_StatisticalCoverageForPMSExceed.SubLocationUnitNumber = FIL_Insert.SUB_LOC_UNIT_NUM AND LKP_StatisticalCoverageForPMSExceed.TypeBureauCode = FIL_Insert.TypeBureauCode AND LKP_StatisticalCoverageForPMSExceed.MaxPolicyCovEffDate = FIL_Insert.CCP_PKG_EFF_DT
	LEFT JOIN LKP_SupTypeOfLossRules
	ON LKP_SupTypeOfLossRules.MajorPerilCode = FIL_Insert.MAJOR_PERIL_CODE AND LKP_SupTypeOfLossRules.CauseOfLoss = FIL_Insert.cause_of_loss_out AND LKP_SupTypeOfLossRules.InsuranceSegmentCode = FIL_Insert.o_InsuranceSegmentCode
),
claimant_coverage_detail_insert AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.claimant_coverage_detail
	(claimant_cov_det_ak_id, claim_party_occurrence_ak_id, s3p_object_type_code, s3p_object_seq_num, s3p_pkg_seq_num, s3p_ins_line_code, s3p_unit_type_code, s3p_wc_class_descript, loc_unit_num, sub_loc_unit_num, ins_line, risk_unit_grp, risk_unit_grp_seq_num, risk_unit, risk_unit_seq_num, major_peril_code, major_peril_seq, pms_loss_disability, reserve_ctgry, cause_of_loss, pms_mbr, pms_type_exposure, pms_type_bureau_code, offset_onset_ind, claimant_cov_eff_date, claimant_cov_exp_date, risk_type_ind, s3p_unit_descript, spec_pers_prop_use_code, pkg_ded_amt, pkg_lmt_amt, manual_entry_ind, unit_veh_registration_state_code, unit_veh_stated_amt, unit_dam_descript, unit_veh_yr, unit_veh_make, unit_vin_num, logical_flag, crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date, SupInsuranceLineID, sup_risk_unit_grp_id, sup_risk_unit_id, SupMajorPerilID, CauseOfLossID, SupTypeBureauCodeID, SupVehicleRegistrationStateID, PolicySourceID, CoverageForm, RiskType, CoverageType, CoverageVersion, AnnualStatementLineNumber, ClassCode, SublineCode, RatingCoverageAKId, CoverageGUID, StatisticalCoverageAKID, InsuranceReferenceLineOfBusinessAKId, ProductAKId, TypeOfLoss, ClaimTypeCategory, ClaimTypeGroup, SubrogationEligibleIndicator)
	SELECT 
	out_claimant_cov_det_ak_id AS CLAIMANT_COV_DET_AK_ID, 
	CLAIM_PARTY_OCCURRENCE_AK_ID, 
	COB_OBJECT_TYPE_CD AS S3P_OBJECT_TYPE_CODE, 
	COB_OBJECT_SEQ_NBR AS S3P_OBJECT_SEQ_NUM, 
	s3p_PKG_SEQ_NUM AS S3P_PKG_SEQ_NUM, 
	CCP_INS_LINE_CD AS S3P_INS_LINE_CODE, 
	COB_UNIT_TYPE_CD AS S3P_UNIT_TYPE_CODE, 
	s3p_WC_CLASS_DESCRIPTION AS S3P_WC_CLASS_DESCRIPT, 
	LOC_UNIT_NUM AS LOC_UNIT_NUM, 
	SUB_LOC_UNIT_NUM AS SUB_LOC_UNIT_NUM, 
	INS_LINE AS INS_LINE, 
	RISK_UNIT_GROUP AS RISK_UNIT_GRP, 
	RISK_UNIT_GRP_SEQ AS RISK_UNIT_GRP_SEQ_NUM, 
	RISK_UNIT AS RISK_UNIT, 
	RISK_UNIT_SEQ AS RISK_UNIT_SEQ_NUM, 
	MAJOR_PERIL_CODE AS MAJOR_PERIL_CODE, 
	MAJOR_PERIL_SEQ_NUM AS MAJOR_PERIL_SEQ, 
	dummy AS PMS_LOSS_DISABILITY, 
	reserve_ctgry_out AS RESERVE_CTGRY, 
	cause_of_loss_out AS CAUSE_OF_LOSS, 
	dummy AS PMS_MBR, 
	dummy AS PMS_TYPE_EXPOSURE, 
	TypeBureauCode AS PMS_TYPE_BUREAU_CODE, 
	dummy AS OFFSET_ONSET_IND, 
	CCP_PKG_EFF_DT AS CLAIMANT_COV_EFF_DATE, 
	CCP_PKG_EXP_DT AS CLAIMANT_COV_EXP_DATE, 
	RISK_TYPE_IND AS RISK_TYPE_IND, 
	s3p_UNIT_DESCRIPTION AS S3P_UNIT_DESCRIPT, 
	SPEC_PERS_PROP_USE_CODE, 
	CCP_PKG_DED_AMT AS PKG_DED_AMT, 
	CCP_PKG_LIMIT_AMT AS PKG_LMT_AMT, 
	CCP_MNL_ENTRY_IND AS MANUAL_ENTRY_IND, 
	CAU_RGS_STATE_CD AS UNIT_VEH_REGISTRATION_STATE_CODE, 
	CAU_VEH_ST_AMT AS UNIT_VEH_STATED_AMT, 
	UNIT_DAMAGE_DESCRIPTION AS UNIT_DAM_DESCRIPT, 
	CAU_CAR_YR AS UNIT_VEH_YR, 
	CAU_CAR_MAKE_NM AS UNIT_VEH_MAKE, 
	CAU_VEHICLE_ID_NBR AS UNIT_VIN_NUM, 
	LOGICAL_FLAG, 
	Crrnt_SnapSht_Flag AS CRRNT_SNPSHT_FLAG, 
	AUDIT_ID AS AUDIT_ID, 
	EFF_FROM_DATE, 
	EFF_TO_DATE, 
	SOURCE_SYSTEM_ID AS SOURCE_SYS_ID, 
	CREATED_DATE, 
	created_date AS MODIFIED_DATE, 
	SUPINSURANCELINEID, 
	SUP_RISK_UNIT_GRP_ID, 
	SUP_RISK_UNIT_ID, 
	SUPMAJORPERILID, 
	CauseOfLossId_out AS CAUSEOFLOSSID, 
	sup_type_bureau_code_id AS SUPTYPEBUREAUCODEID, 
	sup_state_id AS SUPVEHICLEREGISTRATIONSTATEID, 
	cvr_policy_src_id AS POLICYSOURCEID, 
	coverage_form AS COVERAGEFORM, 
	risk_type AS RISKTYPE, 
	coverage_type AS COVERAGETYPE, 
	CoverageVersion_out AS COVERAGEVERSION, 
	AnnualStatementLineNumber_out AS ANNUALSTATEMENTLINENUMBER, 
	ClassCode_out AS CLASSCODE, 
	SublineCode_out AS SUBLINECODE, 
	RatingCoverageAKID_out AS RATINGCOVERAGEAKID, 
	ccp_pol_cov_id_out AS COVERAGEGUID, 
	o_StatisticalCoverageAKId AS STATISTICALCOVERAGEAKID, 
	o_InsuranceReferenceLineOfBusinessAKId AS INSURANCEREFERENCELINEOFBUSINESSAKID, 
	o_ProductAKId AS PRODUCTAKID, 
	o_TypeOfLoss AS TYPEOFLOSS, 
	o_ClaimTypeCategory AS CLAIMTYPECATEGORY, 
	o_ClaimTypeGroup AS CLAIMTYPEGROUP, 
	o_SubrogationEligibleIndicator AS SUBROGATIONELIGIBLEINDICATOR
	FROM EXP_Determine_AK
),
SQ_claimant_coverage_detail_RatingCoverageAKID AS (
	-- get pool of possible changes for a given date period with both the old and new policy information
	-- driven by policyakid from CO for new and PC (via RCAKID from CCD) for old
	with CO_Change as(
	select distinct
	CCD.claimant_cov_det_id,
	CCD.claimant_cov_det_ak_id, 
	CO.claim_occurrence_ak_id, 
	CO.pol_key_ak_id, 
	PC.PolicyAKID as OldPolicyAKID,
	CCD.RatingCoverageAKId as OldRatingCoverageAKId, 
	CCD.CoverageGUID, 
	P.pol_key, 
	P2.Pol_key as OldPolKey,
	CO.claim_loss_date, 
	P.pol_eff_date, 
	case when P.pol_cancellation_date < '2100-12-31' then P.pol_cancellation_date else 	P.pol_exp_date End as pol_exp_date 
	,PC.PolicyCoverageAKID as OldPolicyCoverageAKID
	,P2.pol_eff_date as OldPolEff_date
	,case when P2.pol_cancellation_date < '2100-12-31' then P2.pol_cancellation_date else 	P2.pol_exp_date End as OldPolExp_date 
	,RC.RatingCoverageCancellationDate
	from 
	dbo.claim_occurrence CO  with (nolock)
	inner join dbo.claim_party_occurrence CPO  with (nolock) on CO.claim_occurrence_ak_id=CPO.claim_occurrence_ak_id and CPO.crrnt_snpsht_flag=1
	inner join dbo.claimant_coverage_detail CCD with (nolock) on CCD.claim_party_occurrence_ak_id=CPO.claim_party_occurrence_ak_id and CCD.crrnt_snpsht_flag=1
	inner join v2.policy P with (nolock) on P.pol_ak_id=CO.pol_key_ak_id and P.crrnt_snpsht_flag=1
	inner join dbo.RatingCoverage  RC with (nolock) on RC.RatingCoverageAKID=CCD.RatingCoverageAKId and CCD.CoverageGUID=RC.CoverageGUID and RC.CurrentSnapshotFlag=1
	inner join dbo.PolicyCoverage PC with (nolock) on RC.PolicyCoverageAKID=PC.PolicyCoverageAKID and PC.CurrentSnapshotFlag=1
	inner join v2.policy P2 with (nolock) on P2.pol_ak_id=PC.PolicyAKID and P2.crrnt_snpsht_flag=1
	where CO.modified_date >='@{pipeline().parameters.SELECTION_START_TS}'
	and CO.crrnt_snpsht_flag=1 and PolicySourceID !='PMS' and PC.PolicyAKID!=CO.pol_key_ak_id
	)
	
	-- get current RC information with a join to PC policyakid for a integrity check 
	-- and also provide enough information for a debug/validation query to test old vs new RCAKID
	select 
	distinct 
	CTE.claimant_cov_det_id,
	CTE.claimant_cov_det_ak_id,
	CTE.claim_occurrence_ak_id,
	RC.RatingCoverageAKID as new_RatingCoverageAKID
	,PC.PolicyCoverageAKID as new_PolicyCoverageAKID
	,PC.PolicyAKID
	,CTE.pol_key as new_Pol_key
	,RC.CoverageGUID
	,PC.PolicyCoverageEffectiveDate
	,PC.PolicyCoverageExpirationDate 
	,CTE.pol_eff_date
	,CTE.pol_exp_date
	,RC.RatingCoverageCancellationDate
	,CTE.claim_loss_date
	,CTE.OldPolicyAKID as OLD_PolicyAKID
	,CTE.OldPolKey as OLD_pol_key, 
	CTE.OldPolicyCoverageAKID as OLD_PolicyCoverageAKID,
	CTE.OldRatingCoverageAKId as OLD_RatingCoverageAKID,
	CTE.OldPolEff_date as OLD_pol_eff_date,
	CTE.OldPolExp_date as OLD_pol_exp_date,
	CTE.RatingCoverageCancellationDate as OLD_RatingCoverageCancellationDate
	FROM
	CO_Change CTE 
	Inner join dbo.RatingCoverage RC with (nolock) on 
	CTE.CoverageGUID=RC.CoverageGUID and RC.CurrentSnapshotFlag=1 and CTE.OldRatingCoverageAKId!=RC.RatingCoverageAKID
	inner join dbo.PolicyCoverage PC with (nolock) on RC.PolicyCoverageAKID=PC.PolicyCoverageAKID 
	and CTE.pol_key_ak_id=PC.PolicyAKID
	and PC.CurrentSnapshotFlag=1
	order by CTE.claimant_cov_det_id
),
EXP_ClaimantCoverageDetailRatingCoverageAKID AS (
	SELECT
	claimant_cov_det_id,
	claimant_cov_det_ak_id,
	claim_occurrence_ak_id,
	new_RatingCoverageAKID,
	new_PolicyCoverageAKID,
	PolicyAKID,
	new_Pol_key,
	CoverageGUID,
	PolicyCoverageEffectiveDate,
	PolicyCoverageExpirationDate,
	pol_eff_date,
	pol_exp_date,
	RatingCoverageCancellationDate,
	claim_loss_date,
	OLD_PolicyAKID,
	OLD_pol_key,
	OLD_PolicyCoverageAKID,
	OLD_RatingCoverageAKID,
	OLD_pol_eff_date,
	OLD_pol_exp_date,
	OLD_RatingCoverageCancellationDate
	FROM SQ_claimant_coverage_detail_RatingCoverageAKID
),
SQ_Claimant_Coverage_Detail_Base_Record AS (
	SELECT 
	claimant_coverage_detail.claimant_cov_det_id, 
	claimant_coverage_detail.claimant_cov_det_ak_id, 
	claimant_coverage_detail.claim_party_occurrence_ak_id, 
	claimant_coverage_detail.s3p_object_type_code, 
	claimant_coverage_detail.s3p_object_seq_num, 
	claimant_coverage_detail.s3p_pkg_seq_num, 
	claimant_coverage_detail.s3p_ins_line_code, 
	claimant_coverage_detail.s3p_unit_type_code, 
	claimant_coverage_detail.s3p_wc_class_descript, 
	claimant_coverage_detail.loc_unit_num, 
	claimant_coverage_detail.sub_loc_unit_num, 
	claimant_coverage_detail.ins_line, 
	claimant_coverage_detail.risk_unit_grp, 
	claimant_coverage_detail.risk_unit_grp_seq_num,
	claimant_coverage_detail.risk_unit, 
	claimant_coverage_detail.risk_unit_seq_num, 
	claimant_coverage_detail.major_peril_code, 
	claimant_coverage_detail.major_peril_seq, 
	claimant_coverage_detail.pms_loss_disability, 
	claimant_coverage_detail.reserve_ctgry, 
	claimant_coverage_detail.cause_of_loss, 
	claimant_coverage_detail.pms_mbr, 
	claimant_coverage_detail.pms_type_exposure, 
	claimant_coverage_detail.pms_type_bureau_code, 
	claimant_coverage_detail.offset_onset_ind, 
	claimant_coverage_detail.claimant_cov_eff_date, 
	claimant_coverage_detail.claimant_cov_exp_date, 
	claimant_coverage_detail.risk_type_ind, 
	claimant_coverage_detail.s3p_unit_descript, 
	claimant_coverage_detail.spec_pers_prop_use_code, 
	claimant_coverage_detail.pkg_ded_amt, 
	claimant_coverage_detail.pkg_lmt_amt, 
	claimant_coverage_detail.manual_entry_ind, 
	claimant_coverage_detail.unit_veh_registration_state_code, 
	claimant_coverage_detail.unit_veh_stated_amt, 
	claimant_coverage_detail.unit_dam_descript, 
	claimant_coverage_detail.unit_veh_yr, 
	claimant_coverage_detail.unit_veh_make, 
	claimant_coverage_detail.unit_vin_num, 
	claimant_coverage_detail.logical_flag, 
	claimant_coverage_detail.crrnt_snpsht_flag, 
	claimant_coverage_detail.audit_id, 
	claimant_coverage_detail.eff_from_date, 
	claimant_coverage_detail.eff_to_date,
	claimant_coverage_detail.source_sys_id, 
	claimant_coverage_detail.created_date, 
	claimant_coverage_detail.modified_date, 
	claimant_coverage_detail.SupInsuranceLineID, 
	claimant_coverage_detail.sup_risk_unit_grp_id, 
	claimant_coverage_detail.sup_risk_unit_id, 
	claimant_coverage_detail.SupMajorPerilID, 
	claimant_coverage_detail.CauseOfLossID, 
	claimant_coverage_detail.SupTypeBureauCodeID, 
	claimant_coverage_detail.SupVehicleRegistrationStateID, 
	claimant_coverage_detail.PolicySourceID, 
	claimant_coverage_detail.CoverageForm, 
	claimant_coverage_detail.RiskType, 
	claimant_coverage_detail.CoverageType, 
	claimant_coverage_detail.CoverageVersion, 
	claimant_coverage_detail.AnnualStatementLineNumber, 
	claimant_coverage_detail.ClassCode, 
	claimant_coverage_detail.SublineCode, 
	claimant_coverage_detail.RatingCoverageAKId, 
	claimant_coverage_detail.CoverageGUID, 
	claimant_coverage_detail.StatisticalCoverageAKID, 
	claimant_coverage_detail.InsuranceReferenceLineOfBusinessAKId, 
	claimant_coverage_detail.ProductAKId, 
	claimant_coverage_detail.TypeOfLoss, 
	claimant_coverage_detail.ClaimTypeCategory,
	claimant_coverage_detail.ClaimTypeGroup, 
	claimant_coverage_detail.SubrogationEligibleIndicator 
	FROM
	claimant_coverage_detail claimant_coverage_detail
	INNER JOIN claim_party_occurrence CPO on CPO.claim_party_occurrence_ak_id=claimant_coverage_detail.claim_party_occurrence_ak_id and CPO.crrnt_snpsht_flag=1
	INNER JOIN claim_occurrence CO on CO.claim_occurrence_ak_id=CPO.claim_occurrence_ak_id and CO.crrnt_snpsht_flag=1
	WHERE 
	CO.modified_date >='@{pipeline().parameters.SELECTION_START_TS}'
	and CO.crrnt_snpsht_flag=1 and claimant_coverage_detail.PolicySourceID !='PMS' 
	and claimant_coverage_detail.crrnt_snpsht_flag=1
	ORDER BY claimant_coverage_detail.claimant_cov_det_id
),
JNR_ClaimantCoverageDetailRatingCoverageAKID AS (SELECT
	EXP_ClaimantCoverageDetailRatingCoverageAKID.claimant_cov_det_id AS original_claimant_cov_det_id, 
	EXP_ClaimantCoverageDetailRatingCoverageAKID.claimant_cov_det_ak_id AS original_claimant_cov_det_ak_id, 
	EXP_ClaimantCoverageDetailRatingCoverageAKID.new_RatingCoverageAKID, 
	SQ_Claimant_Coverage_Detail_Base_Record.claimant_cov_det_id, 
	SQ_Claimant_Coverage_Detail_Base_Record.claimant_cov_det_ak_id, 
	SQ_Claimant_Coverage_Detail_Base_Record.claim_party_occurrence_ak_id, 
	SQ_Claimant_Coverage_Detail_Base_Record.s3p_object_type_code, 
	SQ_Claimant_Coverage_Detail_Base_Record.s3p_object_seq_num, 
	SQ_Claimant_Coverage_Detail_Base_Record.s3p_pkg_seq_num, 
	SQ_Claimant_Coverage_Detail_Base_Record.s3p_ins_line_code, 
	SQ_Claimant_Coverage_Detail_Base_Record.s3p_unit_type_code, 
	SQ_Claimant_Coverage_Detail_Base_Record.s3p_wc_class_descript, 
	SQ_Claimant_Coverage_Detail_Base_Record.loc_unit_num, 
	SQ_Claimant_Coverage_Detail_Base_Record.sub_loc_unit_num, 
	SQ_Claimant_Coverage_Detail_Base_Record.ins_line, 
	SQ_Claimant_Coverage_Detail_Base_Record.risk_unit_grp, 
	SQ_Claimant_Coverage_Detail_Base_Record.risk_unit_grp_seq_num, 
	SQ_Claimant_Coverage_Detail_Base_Record.risk_unit, 
	SQ_Claimant_Coverage_Detail_Base_Record.risk_unit_seq_num, 
	SQ_Claimant_Coverage_Detail_Base_Record.major_peril_code, 
	SQ_Claimant_Coverage_Detail_Base_Record.major_peril_seq, 
	SQ_Claimant_Coverage_Detail_Base_Record.pms_loss_disability, 
	SQ_Claimant_Coverage_Detail_Base_Record.reserve_ctgry, 
	SQ_Claimant_Coverage_Detail_Base_Record.cause_of_loss, 
	SQ_Claimant_Coverage_Detail_Base_Record.pms_mbr, 
	SQ_Claimant_Coverage_Detail_Base_Record.pms_type_exposure, 
	SQ_Claimant_Coverage_Detail_Base_Record.pms_type_bureau_code, 
	SQ_Claimant_Coverage_Detail_Base_Record.offset_onset_ind, 
	SQ_Claimant_Coverage_Detail_Base_Record.claimant_cov_eff_date, 
	SQ_Claimant_Coverage_Detail_Base_Record.claimant_cov_exp_date, 
	SQ_Claimant_Coverage_Detail_Base_Record.risk_type_ind, 
	SQ_Claimant_Coverage_Detail_Base_Record.s3p_unit_descript, 
	SQ_Claimant_Coverage_Detail_Base_Record.spec_pers_prop_use_code, 
	SQ_Claimant_Coverage_Detail_Base_Record.pkg_ded_amt, 
	SQ_Claimant_Coverage_Detail_Base_Record.pkg_lmt_amt, 
	SQ_Claimant_Coverage_Detail_Base_Record.manual_entry_ind, 
	SQ_Claimant_Coverage_Detail_Base_Record.unit_veh_registration_state_code, 
	SQ_Claimant_Coverage_Detail_Base_Record.unit_veh_stated_amt, 
	SQ_Claimant_Coverage_Detail_Base_Record.unit_dam_descript, 
	SQ_Claimant_Coverage_Detail_Base_Record.unit_veh_yr, 
	SQ_Claimant_Coverage_Detail_Base_Record.unit_veh_make, 
	SQ_Claimant_Coverage_Detail_Base_Record.unit_vin_num, 
	SQ_Claimant_Coverage_Detail_Base_Record.logical_flag, 
	SQ_Claimant_Coverage_Detail_Base_Record.crrnt_snpsht_flag, 
	SQ_Claimant_Coverage_Detail_Base_Record.audit_id, 
	SQ_Claimant_Coverage_Detail_Base_Record.eff_from_date, 
	SQ_Claimant_Coverage_Detail_Base_Record.eff_to_date, 
	SQ_Claimant_Coverage_Detail_Base_Record.source_sys_id, 
	SQ_Claimant_Coverage_Detail_Base_Record.created_date, 
	SQ_Claimant_Coverage_Detail_Base_Record.modified_date, 
	SQ_Claimant_Coverage_Detail_Base_Record.SupInsuranceLineID, 
	SQ_Claimant_Coverage_Detail_Base_Record.sup_risk_unit_grp_id, 
	SQ_Claimant_Coverage_Detail_Base_Record.sup_risk_unit_id, 
	SQ_Claimant_Coverage_Detail_Base_Record.SupMajorPerilID, 
	SQ_Claimant_Coverage_Detail_Base_Record.CauseOfLossID, 
	SQ_Claimant_Coverage_Detail_Base_Record.SupTypeBureauCodeID, 
	SQ_Claimant_Coverage_Detail_Base_Record.SupVehicleRegistrationStateID, 
	SQ_Claimant_Coverage_Detail_Base_Record.PolicySourceID, 
	SQ_Claimant_Coverage_Detail_Base_Record.CoverageForm, 
	SQ_Claimant_Coverage_Detail_Base_Record.RiskType, 
	SQ_Claimant_Coverage_Detail_Base_Record.CoverageType, 
	SQ_Claimant_Coverage_Detail_Base_Record.CoverageVersion, 
	SQ_Claimant_Coverage_Detail_Base_Record.AnnualStatementLineNumber, 
	SQ_Claimant_Coverage_Detail_Base_Record.ClassCode, 
	SQ_Claimant_Coverage_Detail_Base_Record.SublineCode, 
	SQ_Claimant_Coverage_Detail_Base_Record.RatingCoverageAKId, 
	SQ_Claimant_Coverage_Detail_Base_Record.CoverageGUID, 
	SQ_Claimant_Coverage_Detail_Base_Record.StatisticalCoverageAKID, 
	SQ_Claimant_Coverage_Detail_Base_Record.InsuranceReferenceLineOfBusinessAKId, 
	SQ_Claimant_Coverage_Detail_Base_Record.ProductAKId, 
	SQ_Claimant_Coverage_Detail_Base_Record.TypeOfLoss, 
	SQ_Claimant_Coverage_Detail_Base_Record.ClaimTypeCategory, 
	SQ_Claimant_Coverage_Detail_Base_Record.ClaimTypeGroup, 
	SQ_Claimant_Coverage_Detail_Base_Record.SubrogationEligibleIndicator
	FROM EXP_ClaimantCoverageDetailRatingCoverageAKID
	INNER JOIN SQ_Claimant_Coverage_Detail_Base_Record
	ON SQ_Claimant_Coverage_Detail_Base_Record.claimant_cov_det_id = EXP_ClaimantCoverageDetailRatingCoverageAKID.claimant_cov_det_id AND SQ_Claimant_Coverage_Detail_Base_Record.claimant_cov_det_ak_id = EXP_ClaimantCoverageDetailRatingCoverageAKID.claimant_cov_det_ak_id
),
EXP_Output_ClaimantCoverageDetailRatingCoverageAKID AS (
	SELECT
	original_claimant_cov_det_id,
	original_claimant_cov_det_ak_id,
	new_RatingCoverageAKID,
	claimant_cov_det_id,
	claimant_cov_det_ak_id,
	claim_party_occurrence_ak_id,
	s3p_object_type_code,
	s3p_object_seq_num,
	s3p_pkg_seq_num,
	s3p_ins_line_code,
	s3p_unit_type_code,
	s3p_wc_class_descript,
	loc_unit_num,
	sub_loc_unit_num,
	ins_line,
	risk_unit_grp,
	risk_unit_grp_seq_num,
	risk_unit,
	risk_unit_seq_num,
	major_peril_code,
	major_peril_seq,
	pms_loss_disability,
	reserve_ctgry,
	cause_of_loss,
	pms_mbr,
	pms_type_exposure,
	pms_type_bureau_code,
	offset_onset_ind,
	claimant_cov_eff_date,
	claimant_cov_exp_date,
	risk_type_ind,
	s3p_unit_descript,
	spec_pers_prop_use_code,
	pkg_ded_amt,
	pkg_lmt_amt,
	manual_entry_ind,
	unit_veh_registration_state_code,
	unit_veh_stated_amt,
	unit_dam_descript,
	unit_veh_yr,
	unit_veh_make,
	unit_vin_num,
	logical_flag,
	crrnt_snpsht_flag,
	audit_id,
	eff_from_date,
	eff_to_date,
	source_sys_id,
	created_date,
	modified_date,
	SupInsuranceLineID,
	sup_risk_unit_grp_id,
	sup_risk_unit_id,
	SupMajorPerilID,
	CauseOfLossID,
	SupTypeBureauCodeID,
	SupVehicleRegistrationStateID,
	PolicySourceID,
	CoverageForm,
	RiskType,
	CoverageType,
	CoverageVersion,
	AnnualStatementLineNumber,
	ClassCode,
	SublineCode,
	RatingCoverageAKId,
	CoverageGUID,
	StatisticalCoverageAKID,
	InsuranceReferenceLineOfBusinessAKId,
	ProductAKId,
	TypeOfLoss,
	ClaimTypeCategory,
	ClaimTypeGroup,
	SubrogationEligibleIndicator,
	SYSDATE AS EffFromDate,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditID
	FROM JNR_ClaimantCoverageDetailRatingCoverageAKID
),
Insert_claimant_coverage_detail_RatingCoverageAKID AS (
	INSERT INTO claimant_coverage_detail
	(claimant_cov_det_ak_id, claim_party_occurrence_ak_id, s3p_object_type_code, s3p_object_seq_num, s3p_pkg_seq_num, s3p_ins_line_code, s3p_unit_type_code, s3p_wc_class_descript, loc_unit_num, sub_loc_unit_num, ins_line, risk_unit_grp, risk_unit_grp_seq_num, risk_unit, risk_unit_seq_num, major_peril_code, major_peril_seq, pms_loss_disability, reserve_ctgry, cause_of_loss, pms_mbr, pms_type_exposure, pms_type_bureau_code, offset_onset_ind, claimant_cov_eff_date, claimant_cov_exp_date, risk_type_ind, s3p_unit_descript, spec_pers_prop_use_code, pkg_ded_amt, pkg_lmt_amt, manual_entry_ind, unit_veh_registration_state_code, unit_veh_stated_amt, unit_dam_descript, unit_veh_yr, unit_veh_make, unit_vin_num, logical_flag, crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date, SupInsuranceLineID, sup_risk_unit_grp_id, sup_risk_unit_id, SupMajorPerilID, CauseOfLossID, SupTypeBureauCodeID, SupVehicleRegistrationStateID, PolicySourceID, CoverageForm, RiskType, CoverageType, CoverageVersion, AnnualStatementLineNumber, ClassCode, SublineCode, RatingCoverageAKId, CoverageGUID, StatisticalCoverageAKID, InsuranceReferenceLineOfBusinessAKId, ProductAKId, TypeOfLoss, ClaimTypeCategory, ClaimTypeGroup, SubrogationEligibleIndicator)
	SELECT 
	original_claimant_cov_det_ak_id AS CLAIMANT_COV_DET_AK_ID, 
	CLAIM_PARTY_OCCURRENCE_AK_ID, 
	S3P_OBJECT_TYPE_CODE, 
	S3P_OBJECT_SEQ_NUM, 
	S3P_PKG_SEQ_NUM, 
	S3P_INS_LINE_CODE, 
	S3P_UNIT_TYPE_CODE, 
	S3P_WC_CLASS_DESCRIPT, 
	LOC_UNIT_NUM, 
	SUB_LOC_UNIT_NUM, 
	INS_LINE, 
	RISK_UNIT_GRP, 
	RISK_UNIT_GRP_SEQ_NUM, 
	RISK_UNIT, 
	RISK_UNIT_SEQ_NUM, 
	MAJOR_PERIL_CODE, 
	MAJOR_PERIL_SEQ, 
	PMS_LOSS_DISABILITY, 
	RESERVE_CTGRY, 
	CAUSE_OF_LOSS, 
	PMS_MBR, 
	PMS_TYPE_EXPOSURE, 
	PMS_TYPE_BUREAU_CODE, 
	OFFSET_ONSET_IND, 
	CLAIMANT_COV_EFF_DATE, 
	CLAIMANT_COV_EXP_DATE, 
	RISK_TYPE_IND, 
	S3P_UNIT_DESCRIPT, 
	SPEC_PERS_PROP_USE_CODE, 
	PKG_DED_AMT, 
	PKG_LMT_AMT, 
	MANUAL_ENTRY_IND, 
	UNIT_VEH_REGISTRATION_STATE_CODE, 
	UNIT_VEH_STATED_AMT, 
	UNIT_DAM_DESCRIPT, 
	UNIT_VEH_YR, 
	UNIT_VEH_MAKE, 
	UNIT_VIN_NUM, 
	LOGICAL_FLAG, 
	CRRNT_SNPSHT_FLAG, 
	AuditID AS AUDIT_ID, 
	EffFromDate AS EFF_FROM_DATE, 
	EFF_TO_DATE, 
	SOURCE_SYS_ID, 
	EffFromDate AS CREATED_DATE, 
	EffFromDate AS MODIFIED_DATE, 
	SUPINSURANCELINEID, 
	SUP_RISK_UNIT_GRP_ID, 
	SUP_RISK_UNIT_ID, 
	SUPMAJORPERILID, 
	CAUSEOFLOSSID, 
	SUPTYPEBUREAUCODEID, 
	SUPVEHICLEREGISTRATIONSTATEID, 
	POLICYSOURCEID, 
	COVERAGEFORM, 
	RISKTYPE, 
	COVERAGETYPE, 
	COVERAGEVERSION, 
	ANNUALSTATEMENTLINENUMBER, 
	CLASSCODE, 
	SUBLINECODE, 
	new_RatingCoverageAKID AS RATINGCOVERAGEAKID, 
	COVERAGEGUID, 
	STATISTICALCOVERAGEAKID, 
	INSURANCEREFERENCELINEOFBUSINESSAKID, 
	PRODUCTAKID, 
	TYPEOFLOSS, 
	CLAIMTYPECATEGORY, 
	CLAIMTYPEGROUP, 
	SUBROGATIONELIGIBLEINDICATOR
	FROM EXP_Output_ClaimantCoverageDetailRatingCoverageAKID
),
SQ_claimant_coverage_detail AS (
	SELECT 
	  a.claimant_cov_det_id
	, a.claim_party_occurrence_ak_id
	, a.s3p_object_type_code
	, a.s3p_object_seq_num
	, a.s3p_pkg_seq_num
	, a.major_peril_code
	,a.cause_of_loss
	,a.reserve_ctgry
	, a.eff_from_date
	, a.eff_to_date
	, a.source_sys_id 
	FROM
	 @{pipeline().parameters.TARGET_TABLE_OWNER}.claimant_coverage_detail a
	where a.source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	and EXISTS (SELECT 1			
		FROM  @{pipeline().parameters.TARGET_TABLE_OWNER}.claimant_coverage_detail b
		WHERE b.crrnt_snpsht_flag = 1
	      AND a.claimant_cov_det_ak_id = b.claimant_cov_det_ak_id
	      AND a.source_sys_id = b.source_sys_id
		GROUP BY b.claimant_cov_det_ak_id
		HAVING COUNT(*) > 1)
	order by a.claimant_cov_det_ak_id, a.eff_from_date desc
	--EXISTS Subquery exists to pick AK Groups that have multiple rows with a 12/31/2100 eff_to_date.
	--When this condition occurs this is an indication that we must expire one or more of these rows.
	--WHERE clause is always made up of current snapshot flag and all columns of the AK
	--GROUP BY clause is always on AK
	--HAVING clause stays the same
	
	--ORDER BY of main query orders all rows first by the AK and then by the eff_from_date in a DESC format
	--the descending order is important because this allows us to avoid another lookup and properly apply the eff_to_date by utilizing a local variable to keep track
),
EXP_Expire_Rows AS (
	SELECT
	claimant_cov_det_id,
	claim_party_occurrence_ak_id,
	s3p_object_type_code,
	s3p_object_seq_num,
	s3p_pkg_seq_num,
	major_peril_code,
	reserve_ctgry AS pms_reserve_ctgry,
	cause_of_loss AS pms_loss_cause,
	eff_from_date,
	eff_to_date AS orig_eff_to_date,
	source_sys_id,
	-- *INF*: DECODE (TRUE, 
	-- claim_party_occurrence_ak_id = v_PREV_ROW_claim_party_occurrence_ak_id and
	-- s3p_object_type_code=v_PREV_ROW_s3p_object_type_code and
	-- s3p_object_seq_num=v_PREV_ROW_object_seq_num and
	-- major_peril_code=v_PREV_ROW_major_peril_code and
	-- s3p_pkg_seq_num=v_PREV_ROW_s3p_pkg_seq_num and
	-- pms_loss_cause = v_PEV_ROW_pms_loss_cause and
	-- pms_reserve_ctgry = v_PREV_ROW_pms_reserve_ctgry and
	-- source_sys_id = v_PREV_ROW_source_sys_id
	-- , ADD_TO_DATE(v_PREV_ROW_eff_from_date,'SS',-1)
	-- ,orig_eff_to_date)
	DECODE(
	    TRUE,
	    claim_party_occurrence_ak_id = v_PREV_ROW_claim_party_occurrence_ak_id and s3p_object_type_code = v_PREV_ROW_s3p_object_type_code and s3p_object_seq_num = v_PREV_ROW_object_seq_num and major_peril_code = v_PREV_ROW_major_peril_code and s3p_pkg_seq_num = v_PREV_ROW_s3p_pkg_seq_num and pms_loss_cause = v_PEV_ROW_pms_loss_cause and pms_reserve_ctgry = v_PREV_ROW_pms_reserve_ctgry and source_sys_id = v_PREV_ROW_source_sys_id, DATEADD(SECOND,- 1,v_PREV_ROW_eff_from_date),
	    orig_eff_to_date
	) AS v_eff_to_date,
	v_eff_to_date AS eff_to_date,
	claim_party_occurrence_ak_id AS v_PREV_ROW_claim_party_occurrence_ak_id,
	s3p_object_type_code AS v_PREV_ROW_s3p_object_type_code,
	s3p_object_seq_num AS v_PREV_ROW_object_seq_num,
	major_peril_code AS v_PREV_ROW_major_peril_code,
	s3p_pkg_seq_num AS v_PREV_ROW_s3p_pkg_seq_num,
	source_sys_id AS v_PREV_ROW_source_sys_id,
	eff_from_date AS v_PREV_ROW_eff_from_date,
	pms_reserve_ctgry AS v_PREV_ROW_pms_reserve_ctgry,
	pms_loss_cause AS v_PEV_ROW_pms_loss_cause,
	0 AS crrnt_snapshot_flag,
	sysdate AS modified_date
	FROM SQ_claimant_coverage_detail
),
FIL_Claimant_Coverage_Detail AS (
	SELECT
	claimant_cov_det_id, 
	orig_eff_to_date, 
	eff_to_date, 
	crrnt_snapshot_flag, 
	modified_date
	FROM EXP_Expire_Rows
	WHERE orig_eff_to_date != eff_to_date
),
UPD_Update_Target AS (
	SELECT
	claimant_cov_det_id, 
	orig_eff_to_date, 
	eff_to_date, 
	crrnt_snapshot_flag, 
	modified_date
	FROM FIL_Claimant_Coverage_Detail
),
claimant_coverage_detail_update AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.claimant_coverage_detail AS T
	USING UPD_Update_Target AS S
	ON T.claimant_cov_det_id = S.claimant_cov_det_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snapshot_flag, T.eff_to_date = S.eff_to_date, T.modified_date = S.modified_date
),