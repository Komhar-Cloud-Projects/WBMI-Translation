WITH
SQ_DCIMRiskStage AS (
	SELECT
		ExtractDate,
		SourceSystemid,
		LineId,
		IM_RiskId,
		SessionId,
		Id,
		Description,
		IM_CoverageFormXmlId,
		Deleted
	FROM DCIMRiskStage
),
EXP_Metadata AS (
	SELECT
	ExtractDate,
	SourceSystemid,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	LineId,
	IM_RiskId,
	SessionId,
	Id,
	Description,
	IM_CoverageFormXmlId,
	Deleted
	FROM SQ_DCIMRiskStage
),
ArchDCIMRiskStage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ArchDCIMRiskStage
	(ExtractDate, SourceSystemid, AuditId, LineId, IM_RiskId, SessionId, Id, Deleted, Description, IM_CoverageFormXmlId)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
	LINEID, 
	IM_RISKID, 
	SESSIONID, 
	ID, 
	DELETED, 
	DESCRIPTION, 
	IM_COVERAGEFORMXMLID
	FROM EXP_Metadata
),