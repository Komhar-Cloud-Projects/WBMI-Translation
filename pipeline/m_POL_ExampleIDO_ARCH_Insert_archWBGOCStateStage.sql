WITH
SQ_WBGOCStateStage AS (
	SELECT
		WBGOCStateStageId,
		ExtractDate,
		SourceSystemId,
		LineId,
		WBGOCStateId,
		SessionId,
		IsStateUsed,
		StateAbbreviation,
		StateNumber
	FROM WBGOCStateStage
),
EXP_Metadata AS (
	SELECT
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	WBGOCStateStageId,
	ExtractDate,
	SourceSystemId,
	LineId,
	WBGOCStateId,
	SessionId,
	IsStateUsed,
	StateAbbreviation,
	StateNumber
	FROM SQ_WBGOCStateStage
),
ArchWBGOCStateStage AS (
	INSERT INTO ArchWBGOCStateStage
	(ExtractDate, SourceSystemId, AuditId, WBGOCStateStageId, LineId, WBGOCStateId, SessionId, IsStateUsed, StateAbbreviation, StateNumber)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
	WBGOCSTATESTAGEID, 
	LINEID, 
	WBGOCSTATEID, 
	SESSIONID, 
	ISSTATEUSED, 
	STATEABBREVIATION, 
	STATENUMBER
	FROM EXP_Metadata
),