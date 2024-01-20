WITH
SQ_WB_EC_State AS (
	WITH cte_WB_EC_State(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.LineId,
	X.WB_EC_StateId,
	X.SessionId,
	X.CurrentIteration,
	X.IsStateUsed,
	X.StateAbbreviation,
	X.StateNumber
	FROM
	WB_EC_State X
	inner join
	cte_WB_EC_State Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	SYSDATE AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	LineId,
	WB_EC_StateId,
	SessionId,
	CurrentIteration,
	IsStateUsed AS i_IsStateUsed,
	-- *INF*: IIF(i_IsStateUsed = 'T', 1, 0)
	IFF(i_IsStateUsed = 'T', 1, 0) AS o_IsStateUsed,
	StateAbbreviation,
	StateNumber
	FROM SQ_WB_EC_State
),
WBECStateStage AS (
	TRUNCATE TABLE WBECStateStage;
	INSERT INTO WBECStateStage
	(ExtractDate, SourceSystemId, LineId, WB_EC_StateId, SessionId, CurrentIteration, IsStateUsed, StateAbbreviation, StateNumber)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	LINEID, 
	WB_EC_STATEID, 
	SESSIONID, 
	CURRENTITERATION, 
	o_IsStateUsed AS ISSTATEUSED, 
	STATEABBREVIATION, 
	STATENUMBER
	FROM EXP_Metadata
),