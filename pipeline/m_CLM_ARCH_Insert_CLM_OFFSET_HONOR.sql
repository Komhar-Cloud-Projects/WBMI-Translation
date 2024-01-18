WITH
SQ_CLM_OFFSET_HONOR_STAGE AS (
	SELECT CLM_OFFSET_HONOR_STAGE.CLM_OFFSET_HONOR_ID, CLM_OFFSET_HONOR_STAGE.COH_DRAFT_NBR, CLM_OFFSET_HONOR_STAGE.COH_SEQ_NBR, CLM_OFFSET_HONOR_STAGE.COH_TOTAL_CHK_AMT, CLM_OFFSET_HONOR_STAGE.COH_MICRO_ECD_NBR, CLM_OFFSET_HONOR_STAGE.COH_LOSS_HDL_OFC, CLM_OFFSET_HONOR_STAGE.COH_CUST_CLM_NBR, CLM_OFFSET_HONOR_STAGE.COH_TRS_DT, CLM_OFFSET_HONOR_STAGE.COH_CLAIMANT_NM_ID, CLM_OFFSET_HONOR_STAGE.COH_MAIL_TO_NM_ID, CLM_OFFSET_HONOR_STAGE.COH_FIN_TYPE_CD, CLM_OFFSET_HONOR_STAGE.COH_TRS_TYPE_CD, CLM_OFFSET_HONOR_STAGE.COH_TRS_CAT_CD, CLM_OFFSET_HONOR_STAGE.COH_NEW_CLAIM_NBR, CLM_OFFSET_HONOR_STAGE.COH_NEW_DRAFT_NBR, CLM_OFFSET_HONOR_STAGE.COH_ENTRY_OPR_ID, CLM_OFFSET_HONOR_STAGE.COH_UPDATE_OPR_ID, CLM_OFFSET_HONOR_STAGE.COH_CREATE_TS, CLM_OFFSET_HONOR_STAGE.COH_PMSD_TS, CLM_OFFSET_HONOR_STAGE.COH_UPD_TS, CLM_OFFSET_HONOR_STAGE.COH_BUS_CASE_ID, CLM_OFFSET_HONOR_STAGE.COH_TRS_AMT, CLM_OFFSET_HONOR_STAGE.COH_DWL_LOC_ID, CLM_OFFSET_HONOR_STAGE.COH_CAR_YR, CLM_OFFSET_HONOR_STAGE.COH_CAR_MAKE_NM, CLM_OFFSET_HONOR_STAGE.COH_CAR_MODEL_NM, CLM_OFFSET_HONOR_STAGE.COH_COV_TYPE_CD, CLM_OFFSET_HONOR_STAGE.COH_ITEM_DES_ID, CLM_OFFSET_HONOR_STAGE.COH_BUREAU_LOSS_CD, CLM_OFFSET_HONOR_STAGE.COH_DELETE_IND, CLM_OFFSET_HONOR_STAGE.COH_CLAIM_NBR, CLM_OFFSET_HONOR_STAGE.EXTRACT_DATE, CLM_OFFSET_HONOR_STAGE.AS_OF_DATE, CLM_OFFSET_HONOR_STAGE.RECORD_COUNT, CLM_OFFSET_HONOR_STAGE.SOURCE_SYSTEM_ID 
	FROM
	 CLM_OFFSET_HONOR_STAGE
	WHERE
	CLM_OFFSET_HONOR_STAGE.COH_CREATE_TS >= '@{pipeline().parameters.SELECTION_START_TS}'
	OR
	CLM_OFFSET_HONOR_STAGE.COH_UPD_TS >= '@{pipeline().parameters.SELECTION_START_TS}'
),
EXP_CLM_OFFSET_HONOR_STAGE AS (
	SELECT
	CLM_OFFSET_HONOR_ID,
	COH_DRAFT_NBR,
	COH_SEQ_NBR,
	COH_TOTAL_CHK_AMT,
	COH_MICRO_ECD_NBR,
	COH_LOSS_HDL_OFC,
	COH_CUST_CLM_NBR,
	COH_TRS_DT,
	COH_CLAIMANT_NM_ID,
	COH_MAIL_TO_NM_ID,
	COH_FIN_TYPE_CD,
	COH_TRS_TYPE_CD,
	COH_TRS_CAT_CD,
	COH_NEW_CLAIM_NBR,
	COH_NEW_DRAFT_NBR,
	COH_ENTRY_OPR_ID,
	COH_UPDATE_OPR_ID,
	COH_CREATE_TS,
	COH_PMSD_TS,
	COH_UPD_TS,
	COH_BUS_CASE_ID,
	COH_TRS_AMT,
	COH_DWL_LOC_ID,
	COH_CAR_YR,
	COH_CAR_MAKE_NM,
	COH_CAR_MODEL_NM,
	COH_COV_TYPE_CD,
	COH_ITEM_DES_ID,
	COH_BUREAU_LOSS_CD,
	COH_DELETE_IND,
	COH_CLAIM_NBR,
	EXTRACT_DATE,
	AS_OF_DATE,
	RECORD_COUNT,
	SOURCE_SYSTEM_ID,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AUDIT_ID_OP
	FROM SQ_CLM_OFFSET_HONOR_STAGE
),
ARCH_CLM_OFFSET_HONOR_STAGE AS (
	INSERT INTO ARCH_CLM_OFFSET_HONOR_STAGE
	(CLM_OFFSET_HONOR_ID, COH_DRAFT_NBR, COH_SEQ_NBR, COH_TOTAL_CHK_AMT, COH_MICRO_ECD_NBR, COH_LOSS_HDL_OFC, COH_CUST_CLM_NBR, COH_TRS_DT, COH_CLAIMANT_NM_ID, COH_MAIL_TO_NM_ID, COH_FIN_TYPE_CD, COH_TRS_TYPE_CD, COH_TRS_CAT_CD, COH_NEW_CLAIM_NBR, COH_NEW_DRAFT_NBR, COH_ENTRY_OPR_ID, COH_UPDATE_OPR_ID, COH_CREATE_TS, COH_PMSD_TS, COH_UPD_TS, COH_BUS_CASE_ID, COH_TRS_AMT, COH_DWL_LOC_ID, COH_CAR_YR, COH_CAR_MAKE_NM, COH_CAR_MODEL_NM, COH_COV_TYPE_CD, COH_ITEM_DES_ID, COH_BUREAU_LOSS_CD, COH_DELETE_IND, COH_CLAIM_NBR, EXTRACT_DATE, AS_OF_DATE, RECORD_COUNT, SOURCE_SYSTEM_ID, AUDIT_ID)
	SELECT 
	CLM_OFFSET_HONOR_ID, 
	COH_DRAFT_NBR, 
	COH_SEQ_NBR, 
	COH_TOTAL_CHK_AMT, 
	COH_MICRO_ECD_NBR, 
	COH_LOSS_HDL_OFC, 
	COH_CUST_CLM_NBR, 
	COH_TRS_DT, 
	COH_CLAIMANT_NM_ID, 
	COH_MAIL_TO_NM_ID, 
	COH_FIN_TYPE_CD, 
	COH_TRS_TYPE_CD, 
	COH_TRS_CAT_CD, 
	COH_NEW_CLAIM_NBR, 
	COH_NEW_DRAFT_NBR, 
	COH_ENTRY_OPR_ID, 
	COH_UPDATE_OPR_ID, 
	COH_CREATE_TS, 
	COH_PMSD_TS, 
	COH_UPD_TS, 
	COH_BUS_CASE_ID, 
	COH_TRS_AMT, 
	COH_DWL_LOC_ID, 
	COH_CAR_YR, 
	COH_CAR_MAKE_NM, 
	COH_CAR_MODEL_NM, 
	COH_COV_TYPE_CD, 
	COH_ITEM_DES_ID, 
	COH_BUREAU_LOSS_CD, 
	COH_DELETE_IND, 
	COH_CLAIM_NBR, 
	EXTRACT_DATE, 
	AS_OF_DATE, 
	RECORD_COUNT, 
	SOURCE_SYSTEM_ID, 
	AUDIT_ID_OP AS AUDIT_ID
	FROM EXP_CLM_OFFSET_HONOR_STAGE
),