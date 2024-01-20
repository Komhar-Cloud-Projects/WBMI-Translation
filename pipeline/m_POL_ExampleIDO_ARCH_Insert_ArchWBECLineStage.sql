WITH
SQ_WBECLineStage AS (
	SELECT
		WBECLineStageId,
		ExtractDate,
		SourceSystemId,
		LineId,
		WB_CL_LineId,
		WB_EC_LineId,
		SessionId
	FROM WBECLineStage
),
EXP_Metadata AS (
	SELECT
	WBECLineStageId,
	ExtractDate,
	SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	LineId,
	WB_CL_LineId,
	WB_EC_LineId,
	SessionId
	FROM SQ_WBECLineStage
),
ArchWBECLineStage AS (
	INSERT INTO ArchWBECLineStage
	(ExtractDate, SourceSystemId, AuditId, WBECLineStageId, LineId, WB_CL_LineId, WB_EC_LineId, SessionId)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
	WBECLINESTAGEID, 
	LINEID, 
	WB_CL_LINEID, 
	WB_EC_LINEID, 
	SESSIONID
	FROM EXP_Metadata
),