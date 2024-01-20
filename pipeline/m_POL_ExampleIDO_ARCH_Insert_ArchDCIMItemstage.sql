WITH
SQ_DCIMItemstage AS (
	SELECT
		DCIMItemStageId,
		CoverageId,
		IMItemId,
		SessionId,
		Id,
		Type,
		ExtractDate,
		SourceSystemId
	FROM DCIMItemStage
),
EXP_Metadata AS (
	SELECT
	DCIMItemStageId,
	CoverageId,
	IMItemId,
	SessionId,
	Id,
	Type,
	ExtractDate,
	SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM SQ_DCIMItemstage
),
ArchDCIMItemStage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ArchDCIMItemStage
	(DCIMItemStageId, CoverageId, IMItemId, SessionId, Id, Type, ExtractDate, SourceSystemId, AuditId)
	SELECT 
	DCIMITEMSTAGEID, 
	COVERAGEID, 
	IMITEMID, 
	SESSIONID, 
	ID, 
	TYPE, 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID
	FROM EXP_Metadata
),