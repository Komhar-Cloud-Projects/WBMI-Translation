WITH
SQ_WBIMRiskStage AS (
	SELECT
		WBIMRiskStageId AS WBIMRiskStageID,
		IMRiskId AS IM_RiskId,
		WBIMRiskId AS WB_IM_RiskId,
		SessionId,
		IMLocationXmlId AS IM_LocationXmlId,
		PurePremium,
		ExtractDate,
		SourceSystemId
	FROM WBIMRiskStage
),
EXP_Metadata AS (
	SELECT
	WBIMRiskStageID,
	IM_RiskId,
	WB_IM_RiskId,
	SessionId,
	IM_LocationXmlId,
	PurePremium,
	ExtractDate,
	SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM SQ_WBIMRiskStage
),
ArchWBIMRiskStage1 AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ArchWBIMRiskStage
	(WBIMRiskStageId, IMRiskId, WBIMRiskId, SessionId, IMLocationXmlId, PurePremium, ExtractDate, SourceSystemId, AuditId)
	SELECT 
	WBIMRiskStageID AS WBIMRISKSTAGEID, 
	IM_RiskId AS IMRISKID, 
	WB_IM_RiskId AS WBIMRISKID, 
	SESSIONID, 
	IM_LocationXmlId AS IMLOCATIONXMLID, 
	PUREPREMIUM, 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID
	FROM EXP_Metadata
),