WITH
SQ_PROPERTY_LOSS_STAGE AS (
	SELECT PROPERTY_LOSS_STAGE.PROPERTY_LOSS_ID, PROPERTY_LOSS_STAGE.CPR_CLAIM_NBR, PROPERTY_LOSS_STAGE.CPR_OBJECT_TYPE_CD, PROPERTY_LOSS_STAGE.CPR_OBJECT_SEQ_NBR, PROPERTY_LOSS_STAGE.CPR_PPY_DES_ID, PROPERTY_LOSS_STAGE.CPR_BEF_LOSS_AMT, PROPERTY_LOSS_STAGE.CPR_AGREE_ACT_IND, PROPERTY_LOSS_STAGE.CPR_EST_TOTAL_LOSS, PROPERTY_LOSS_STAGE.CPR_PURCHASED_AMT, PROPERTY_LOSS_STAGE.CPR_EST_DAY_REPAIR, PROPERTY_LOSS_STAGE.CPR_PURPOSE_USE_ID, PROPERTY_LOSS_STAGE.CPR_PHD_OWN_IND, PROPERTY_LOSS_STAGE.CPR_ENTRY_OPR_ID, PROPERTY_LOSS_STAGE.CPR_UPDATE_OPR_ID, PROPERTY_LOSS_STAGE.CPR_CREATE_TS, PROPERTY_LOSS_STAGE.CPR_PMSD_TS, PROPERTY_LOSS_STAGE.CPR_UPD_TS, PROPERTY_LOSS_STAGE.CPR_WHEN_SEE_ID, PROPERTY_LOSS_STAGE.CPR_PPY_LOC_ID, PROPERTY_LOSS_STAGE.CPR_DAMAGE_DES_ID, PROPERTY_LOSS_STAGE.CPR_MSC_PPY_IND, PROPERTY_LOSS_STAGE.CPR_ISU_OWNER_IND, PROPERTY_LOSS_STAGE.CPR_BODILY_INJ_NBR, PROPERTY_LOSS_STAGE.CPR_FATALITY_NBR, PROPERTY_LOSS_STAGE.CPR_REAL_VALUE_AMT, PROPERTY_LOSS_STAGE.CPR_CAUSE_OF_FIRE, PROPERTY_LOSS_STAGE.CPR_BURNING_MAT_CD, PROPERTY_LOSS_STAGE.CPR_LEAKING_LIQUID, PROPERTY_LOSS_STAGE.CPR_LKG_DAM_PLC_CD, PROPERTY_LOSS_STAGE.CPR_PURCHASE_DT, PROPERTY_LOSS_STAGE.CPR_CUR_VAL_AMT, PROPERTY_LOSS_STAGE.CPR_EST_WORK_TIME, PROPERTY_LOSS_STAGE.CPR_ACT_DAY_REPAIR, PROPERTY_LOSS_STAGE.CPR_ACT_WORK_TIME, PROPERTY_LOSS_STAGE.CPR_EST_TOTAL_PPY, PROPERTY_LOSS_STAGE.CPR_EST_PPY_NEW, PROPERTY_LOSS_STAGE.CPR_EST_REP_AMT, PROPERTY_LOSS_STAGE.CPR_ACT_TOTAL_LOSS, PROPERTY_LOSS_STAGE.CPR_ACT_PPY_NEW, PROPERTY_LOSS_STAGE.CPR_ACT_REP_AMT, PROPERTY_LOSS_STAGE.CPR_ACT_TOT_PPY, PROPERTY_LOSS_STAGE.CPR_ACT_WORK_AMT, PROPERTY_LOSS_STAGE.CPR_EST_WORK_AMT, PROPERTY_LOSS_STAGE.CPR_OTHER_PPY_ID, PROPERTY_LOSS_STAGE.CPR_DAM_MAC_CD, PROPERTY_LOSS_STAGE.CPR_MUL_USERS_IND, PROPERTY_LOSS_STAGE.CPR_ACL_MAT_AMT, PROPERTY_LOSS_STAGE.CPR_EST_MAT_AMT, PROPERTY_LOSS_STAGE.EXTRACT_DATE, PROPERTY_LOSS_STAGE.AS_OF_DATE, PROPERTY_LOSS_STAGE.RECORD_COUNT, PROPERTY_LOSS_STAGE.SOURCE_SYSTEM_ID 
	FROM
	 PROPERTY_LOSS_STAGE
	WHERE
	PROPERTY_LOSS_STAGE.CPR_CREATE_TS >= '@{pipeline().parameters.SELECTION_START_TS}'
	OR
	PROPERTY_LOSS_STAGE.CPR_UPD_TS >= '@{pipeline().parameters.SELECTION_START_TS}'
),
EXP_PROPERTY_LOSS_STAGE AS (
	SELECT
	PROPERTY_LOSS_ID,
	CPR_CLAIM_NBR,
	CPR_OBJECT_TYPE_CD,
	CPR_OBJECT_SEQ_NBR,
	CPR_PPY_DES_ID,
	CPR_BEF_LOSS_AMT,
	CPR_AGREE_ACT_IND,
	CPR_EST_TOTAL_LOSS,
	CPR_PURCHASED_AMT,
	CPR_EST_DAY_REPAIR,
	CPR_PURPOSE_USE_ID,
	CPR_PHD_OWN_IND,
	CPR_ENTRY_OPR_ID,
	CPR_UPDATE_OPR_ID,
	CPR_CREATE_TS,
	CPR_PMSD_TS,
	CPR_UPD_TS,
	CPR_WHEN_SEE_ID,
	CPR_PPY_LOC_ID,
	CPR_DAMAGE_DES_ID,
	CPR_MSC_PPY_IND,
	CPR_ISU_OWNER_IND,
	CPR_BODILY_INJ_NBR,
	CPR_FATALITY_NBR,
	CPR_REAL_VALUE_AMT,
	CPR_CAUSE_OF_FIRE,
	CPR_BURNING_MAT_CD,
	CPR_LEAKING_LIQUID,
	CPR_LKG_DAM_PLC_CD,
	CPR_PURCHASE_DT,
	CPR_CUR_VAL_AMT,
	CPR_EST_WORK_TIME,
	CPR_ACT_DAY_REPAIR,
	CPR_ACT_WORK_TIME,
	CPR_EST_TOTAL_PPY,
	CPR_EST_PPY_NEW,
	CPR_EST_REP_AMT,
	CPR_ACT_TOTAL_LOSS,
	CPR_ACT_PPY_NEW,
	CPR_ACT_REP_AMT,
	CPR_ACT_TOT_PPY,
	CPR_ACT_WORK_AMT,
	CPR_EST_WORK_AMT,
	CPR_OTHER_PPY_ID,
	CPR_DAM_MAC_CD,
	CPR_MUL_USERS_IND,
	CPR_ACL_MAT_AMT,
	CPR_EST_MAT_AMT,
	EXTRACT_DATE,
	AS_OF_DATE,
	RECORD_COUNT,
	SOURCE_SYSTEM_ID,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AUDIT_ID_OP
	FROM SQ_PROPERTY_LOSS_STAGE
),
ARCH_PROPERTY_LOSS_STAGE AS (
	INSERT INTO ARCH_PROPERTY_LOSS_STAGE
	(PROPERTY_LOSS_ID, CPR_CLAIM_NBR, CPR_OBJECT_TYPE_CD, CPR_OBJECT_SEQ_NBR, CPR_PPY_DES_ID, CPR_BEF_LOSS_AMT, CPR_AGREE_ACT_IND, CPR_EST_TOTAL_LOSS, CPR_PURCHASED_AMT, CPR_EST_DAY_REPAIR, CPR_PURPOSE_USE_ID, CPR_PHD_OWN_IND, CPR_ENTRY_OPR_ID, CPR_UPDATE_OPR_ID, CPR_CREATE_TS, CPR_PMSD_TS, CPR_UPD_TS, CPR_WHEN_SEE_ID, CPR_PPY_LOC_ID, CPR_DAMAGE_DES_ID, CPR_MSC_PPY_IND, CPR_ISU_OWNER_IND, CPR_BODILY_INJ_NBR, CPR_FATALITY_NBR, CPR_REAL_VALUE_AMT, CPR_CAUSE_OF_FIRE, CPR_BURNING_MAT_CD, CPR_LEAKING_LIQUID, CPR_LKG_DAM_PLC_CD, CPR_PURCHASE_DT, CPR_CUR_VAL_AMT, CPR_EST_WORK_TIME, CPR_ACT_DAY_REPAIR, CPR_ACT_WORK_TIME, CPR_EST_TOTAL_PPY, CPR_EST_PPY_NEW, CPR_EST_REP_AMT, CPR_ACT_TOTAL_LOSS, CPR_ACT_PPY_NEW, CPR_ACT_REP_AMT, CPR_ACT_TOT_PPY, CPR_ACT_WORK_AMT, CPR_EST_WORK_AMT, CPR_OTHER_PPY_ID, CPR_DAM_MAC_CD, CPR_MUL_USERS_IND, CPR_ACL_MAT_AMT, CPR_EST_MAT_AMT, EXTRACT_DATE, AS_OF_DATE, RECORD_COUNT, SOURCE_SYSTEM_ID, AUDIT_ID)
	SELECT 
	PROPERTY_LOSS_ID, 
	CPR_CLAIM_NBR, 
	CPR_OBJECT_TYPE_CD, 
	CPR_OBJECT_SEQ_NBR, 
	CPR_PPY_DES_ID, 
	CPR_BEF_LOSS_AMT, 
	CPR_AGREE_ACT_IND, 
	CPR_EST_TOTAL_LOSS, 
	CPR_PURCHASED_AMT, 
	CPR_EST_DAY_REPAIR, 
	CPR_PURPOSE_USE_ID, 
	CPR_PHD_OWN_IND, 
	CPR_ENTRY_OPR_ID, 
	CPR_UPDATE_OPR_ID, 
	CPR_CREATE_TS, 
	CPR_PMSD_TS, 
	CPR_UPD_TS, 
	CPR_WHEN_SEE_ID, 
	CPR_PPY_LOC_ID, 
	CPR_DAMAGE_DES_ID, 
	CPR_MSC_PPY_IND, 
	CPR_ISU_OWNER_IND, 
	CPR_BODILY_INJ_NBR, 
	CPR_FATALITY_NBR, 
	CPR_REAL_VALUE_AMT, 
	CPR_CAUSE_OF_FIRE, 
	CPR_BURNING_MAT_CD, 
	CPR_LEAKING_LIQUID, 
	CPR_LKG_DAM_PLC_CD, 
	CPR_PURCHASE_DT, 
	CPR_CUR_VAL_AMT, 
	CPR_EST_WORK_TIME, 
	CPR_ACT_DAY_REPAIR, 
	CPR_ACT_WORK_TIME, 
	CPR_EST_TOTAL_PPY, 
	CPR_EST_PPY_NEW, 
	CPR_EST_REP_AMT, 
	CPR_ACT_TOTAL_LOSS, 
	CPR_ACT_PPY_NEW, 
	CPR_ACT_REP_AMT, 
	CPR_ACT_TOT_PPY, 
	CPR_ACT_WORK_AMT, 
	CPR_EST_WORK_AMT, 
	CPR_OTHER_PPY_ID, 
	CPR_DAM_MAC_CD, 
	CPR_MUL_USERS_IND, 
	CPR_ACL_MAT_AMT, 
	CPR_EST_MAT_AMT, 
	EXTRACT_DATE, 
	AS_OF_DATE, 
	RECORD_COUNT, 
	SOURCE_SYSTEM_ID, 
	AUDIT_ID_OP AS AUDIT_ID
	FROM EXP_PROPERTY_LOSS_STAGE
),