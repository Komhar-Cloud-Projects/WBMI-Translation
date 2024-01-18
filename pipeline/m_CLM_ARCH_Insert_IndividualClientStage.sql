WITH
SQ_IndividualClientStage AS (
	SELECT [IndividualClientStageId],
		   [ExtractDate],
		   [SourceSystemId],
		   [CLIENT_ID],
		   [HISTORY_VLD_NBR],
		   [CIID_EFF_DT],
		   [CIID_BIR_CIT_NM],
		   [CIID_BIR_CTR_CD],
		   [CIID_DTH_DT],
		   [EDU_LVL_CD],
		   [GRS_SAL_CD],
		   [MRS_CD],
		   [CIID_NBR_DPN],
		   [OCP_CD],
		   [CIID_CZN_CTR_CD],
		   [CIID_NCZ_ARV_DT],
		   [FIN_WORTH_CD],
		   [CIID_OWN_RENT_CODE],
		   [CIID_YEAR_HIRED],
		   [CIID_HEIGHT],
		   [CIID_HGT_UNITS_CD],
		   [CIID_WEIGHT],
		   [CIID_WGT_UNITS_CD],
		   [CIID_POS_TITLE_TXT],
		   [USER_ID],
		   [STATUS_CD],
		   [TERMINAL_ID],
		   [CIID_EXP_DT],
		   [CIID_EFF_ACY_TS],
		   [CIID_EXP_ACY_TS]
	FROM dbo.IndividualClientStage
	WHERE CIID_EFF_ACY_TS >= '@{pipeline().parameters.SELECTION_START_TS}'
),
EXPTRANS AS (
	SELECT
	IndividualClientStageId,
	ExtractDate,
	SourceSystemId,
	CLIENT_ID,
	HISTORY_VLD_NBR,
	CIID_EFF_DT,
	CIID_BIR_CIT_NM,
	CIID_BIR_CTR_CD,
	CIID_DTH_DT,
	EDU_LVL_CD,
	GRS_SAL_CD,
	MRS_CD,
	CIID_NBR_DPN,
	OCP_CD,
	CIID_CZN_CTR_CD,
	CIID_NCZ_ARV_DT,
	FIN_WORTH_CD,
	CIID_OWN_RENT_CODE,
	CIID_YEAR_HIRED,
	CIID_HEIGHT,
	CIID_HGT_UNITS_CD,
	CIID_WEIGHT,
	CIID_WGT_UNITS_CD,
	CIID_POS_TITLE_TXT,
	USER_ID,
	STATUS_CD,
	TERMINAL_ID,
	CIID_EXP_DT,
	CIID_EFF_ACY_TS,
	CIID_EXP_ACY_TS,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditId
	FROM SQ_IndividualClientStage
),
ArchIndividualClientStage AS (
	INSERT INTO ArchIndividualClientStage
	(AuditId, IndividualClientStageId, ExtractDate, SourceSystemId, CLIENT_ID, HISTORY_VLD_NBR, CIID_EFF_DT, CIID_BIR_CIT_NM, CIID_BIR_CTR_CD, CIID_DTH_DT, EDU_LVL_CD, GRS_SAL_CD, MRS_CD, CIID_NBR_DPN, OCP_CD, CIID_CZN_CTR_CD, CIID_NCZ_ARV_DT, FIN_WORTH_CD, CIID_OWN_RENT_CODE, CIID_YEAR_HIRED, CIID_HEIGHT, CIID_HGT_UNITS_CD, CIID_WEIGHT, CIID_WGT_UNITS_CD, CIID_POS_TITLE_TXT, USER_ID, STATUS_CD, TERMINAL_ID, CIID_EXP_DT, CIID_EFF_ACY_TS, CIID_EXP_ACY_TS)
	SELECT 
	AUDITID, 
	INDIVIDUALCLIENTSTAGEID, 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	CLIENT_ID, 
	HISTORY_VLD_NBR, 
	CIID_EFF_DT, 
	CIID_BIR_CIT_NM, 
	CIID_BIR_CTR_CD, 
	CIID_DTH_DT, 
	EDU_LVL_CD, 
	GRS_SAL_CD, 
	MRS_CD, 
	CIID_NBR_DPN, 
	OCP_CD, 
	CIID_CZN_CTR_CD, 
	CIID_NCZ_ARV_DT, 
	FIN_WORTH_CD, 
	CIID_OWN_RENT_CODE, 
	CIID_YEAR_HIRED, 
	CIID_HEIGHT, 
	CIID_HGT_UNITS_CD, 
	CIID_WEIGHT, 
	CIID_WGT_UNITS_CD, 
	CIID_POS_TITLE_TXT, 
	USER_ID, 
	STATUS_CD, 
	TERMINAL_ID, 
	CIID_EXP_DT, 
	CIID_EFF_ACY_TS, 
	CIID_EXP_ACY_TS
	FROM EXPTRANS
),