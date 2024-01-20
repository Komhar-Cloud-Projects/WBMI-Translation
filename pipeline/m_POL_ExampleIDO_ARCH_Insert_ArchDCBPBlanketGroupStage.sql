WITH
SQ_DCBPBlanketGroupStage AS (
	SELECT
		DCBPBlanketGroupStageId,
		ExtractDate,
		SourceSystemId,
		BP_RiskId,
		BP_BlanketGroupId,
		SessionId,
		Id,
		ARate,
		Type
	FROM DCBPBlanketGroupStage
),
EXP_Metadata AS (
	SELECT
	DCBPBlanketGroupStageId,
	ExtractDate,
	SourceSystemId,
	BP_RiskId,
	BP_BlanketGroupId,
	SessionId,
	Id,
	ARate,
	Type,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM SQ_DCBPBlanketGroupStage
),
ArchDCBPBlanketGroupStage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ArchDCBPBlanketGroupStage
	(ExtractDate, SourceSystemId, AuditId, DCBPBlanketGroupStageId, BP_RiskId, BP_BlanketGroupId, SessionId, Id, ARate, Type)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
	DCBPBLANKETGROUPSTAGEID, 
	BP_RISKID, 
	BP_BLANKETGROUPID, 
	SESSIONID, 
	ID, 
	ARATE, 
	TYPE
	FROM EXP_Metadata
),