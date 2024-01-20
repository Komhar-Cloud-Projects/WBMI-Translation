WITH
SQ_WBHIOStateStage AS (
	SELECT
		WBHIOStateStageId,
		LineId,
		WBHIOStateId,
		SessionId,
		IsStateUsed,
		StateAbbreviation,
		StateNumber,
		ExtractDate,
		SourceSystemId
	FROM WBHIOStateStage
),
EXP_Metadata AS (
	SELECT
	WBHIOStateStageId,
	LineId,
	WBHIOStateId,
	SessionId,
	IsStateUsed AS i_IsStateUsed,
	-- *INF*: IIF(i_IsStateUsed='T','1','0')
	IFF(i_IsStateUsed = 'T', '1', '0') AS o_IsStateUsed,
	StateAbbreviation,
	StateNumber,
	ExtractDate,
	SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM SQ_WBHIOStateStage
),
ArchWBHIOStateStage AS (
	INSERT INTO ArchWBHIOStateStage
	(ExtractDate, SourceSystemId, AuditId, WBHIOStateStageId, LineId, WBHIOStateId, SessionId, IsStateUsed, StateAbbreviation, StateNumber)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
	WBHIOSTATESTAGEID, 
	LINEID, 
	WBHIOSTATEID, 
	SESSIONID, 
	o_IsStateUsed AS ISSTATEUSED, 
	STATEABBREVIATION, 
	STATENUMBER
	FROM EXP_Metadata
),