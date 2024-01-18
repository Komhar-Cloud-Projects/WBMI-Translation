WITH
SQ_WBECStateStage AS (
	SELECT
		WBECStateStageId,
		ExtractDate,
		SourceSystemId,
		LineId,
		WB_EC_StateId,
		SessionId,
		CurrentIteration,
		IsStateUsed,
		StateAbbreviation,
		StateNumber
	FROM WBECStateStage
),
EXP_Metadata AS (
	SELECT
	WBECStateStageId,
	ExtractDate,
	SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	LineId,
	WB_EC_StateId,
	SessionId,
	CurrentIteration,
	IsStateUsed AS i_IsStateUsed,
	-- *INF*: IIF(i_IsStateUsed = 'T', 1, 0)
	IFF(i_IsStateUsed = 'T', 1, 0) AS o_IsStateUsed,
	StateAbbreviation,
	StateNumber
	FROM SQ_WBECStateStage
),
ArchWBECStateStage AS (
	INSERT INTO ArchWBECStateStage
	(ExtractDate, SourceSystemId, AuditId, WBECStateStageId, LineId, WB_EC_StateId, SessionId, CurrentIteration, IsStateUsed, StateAbbreviation, StateNumber)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
	WBECSTATESTAGEID, 
	LINEID, 
	WB_EC_STATEID, 
	SESSIONID, 
	CURRENTITERATION, 
	o_IsStateUsed AS ISSTATEUSED, 
	STATEABBREVIATION, 
	STATENUMBER
	FROM EXP_Metadata
),