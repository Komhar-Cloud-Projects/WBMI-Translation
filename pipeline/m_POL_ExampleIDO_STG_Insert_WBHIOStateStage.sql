WITH
SQ_WB_HIO_State AS (
	WITH cte_WBHIOState(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.LineId, 
	X.WB_HIO_StateId, 
	X.SessionId, 
	X.IsStateUsed, 
	X.StateAbbreviation, 
	X.StateNumber 
	FROM
	WB_HIO_State X
	inner join
	cte_WBHIOState Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	LineId,
	WB_HIO_StateId,
	SessionId,
	IsStateUsed AS i_IsStateUsed,
	-- *INF*: IIF(i_IsStateUsed='T',1,0)
	IFF(i_IsStateUsed = 'T', 1, 0) AS o_IsStateUsed,
	StateAbbreviation,
	StateNumber,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_WB_HIO_State
),
WBHIOStateStage AS (
	TRUNCATE TABLE WBHIOStateStage;
	INSERT INTO WBHIOStateStage
	(ExtractDate, SourceSystemId, LineId, WBHIOStateId, SessionId, IsStateUsed, StateAbbreviation, StateNumber)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	LINEID, 
	WB_HIO_StateId AS WBHIOSTATEID, 
	SESSIONID, 
	o_IsStateUsed AS ISSTATEUSED, 
	STATEABBREVIATION, 
	STATENUMBER
	FROM EXP_Metadata
),