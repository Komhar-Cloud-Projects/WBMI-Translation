WITH
SQ_WBIMStateStage AS (
	SELECT
		WBIMStateStageId,
		ExtractDate,
		SourceSystemId,
		WBIMLineId,
		WBIMStateId,
		SessionId,
		StateAbbreviation,
		IsStateUsed,
		StateNumber
	FROM WBIMStateStage
),
EXP_Metadata AS (
	SELECT
	WBIMStateStageId,
	ExtractDate,
	SourceSystemId,
	WBIMLineId,
	WBIMStateId,
	SessionId,
	StateAbbreviation,
	IsStateUsed,
	-- *INF*: DECODE(IsStateUsed, 'T', 1, 'F', 0, NULL)
	DECODE(
	    IsStateUsed,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_IsStateUsed,
	StateNumber,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM SQ_WBIMStateStage
),
ArchWBIMStateStage AS (
	INSERT INTO ArchWBIMStateStage
	(ExtractDate, SourceSystemId, AuditId, WBIMStateStageId, WBIMLineId, WBIMStateId, SessionId, StateAbbreviation, IsStateUsed, StateNumber)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
	WBIMSTATESTAGEID, 
	WBIMLINEID, 
	WBIMSTATEID, 
	SESSIONID, 
	STATEABBREVIATION, 
	o_IsStateUsed AS ISSTATEUSED, 
	STATENUMBER
	FROM EXP_Metadata
),