WITH
SQ_WBHIORiskStage AS (
	SELECT
		WBHIORiskStageId,
		LineId,
		WBHIORiskId,
		SessionId,
		LocationId,
		ExtractDate,
		SourceSystemId
	FROM WBHIORiskStage
),
EXP_Metadata AS (
	SELECT
	WBHIORiskStageId,
	LineId,
	WBHIORiskId,
	SessionId,
	LocationId,
	ExtractDate,
	SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM SQ_WBHIORiskStage
),
ArchWBHIORiskStage AS (
	INSERT INTO ArchWBHIORiskStage
	(ExtractDate, SourceSystemId, AuditId, WBHIORiskStageId, LineId, WBHIORiskId, SessionId, LocationId)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
	WBHIORISKSTAGEID, 
	LINEID, 
	WBHIORISKID, 
	SESSIONID, 
	LOCATIONID
	FROM EXP_Metadata
),