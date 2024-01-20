WITH
SQ_WBECRiskStage AS (
	SELECT
		WBECRiskStageId,
		ExtractDate,
		SourceSystemId,
		LineId,
		WB_EC_RiskId,
		SessionId,
		LocationId
	FROM WBECRiskStage
),
EXP_Metadata AS (
	SELECT
	WBECRiskStageId,
	ExtractDate,
	SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	LineId,
	WB_EC_RiskId,
	SessionId,
	LocationId
	FROM SQ_WBECRiskStage
),
ArchWBECRiskStage AS (
	INSERT INTO ArchWBECRiskStage
	(ExtractDate, SourceSystemId, AuditId, WBECRiskStageId, LineId, WB_EC_RiskId, SessionId, LocationId)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
	WBECRISKSTAGEID, 
	LINEID, 
	WB_EC_RISKID, 
	SESSIONID, 
	LOCATIONID
	FROM EXP_Metadata
),