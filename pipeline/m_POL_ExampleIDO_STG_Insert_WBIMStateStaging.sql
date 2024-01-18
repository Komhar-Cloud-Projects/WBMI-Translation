WITH
SQ_WB_IM_State AS (
	WITH cte_WBIMState(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.WB_IM_LineId, 
	X.WB_IM_StateId, 
	X.SessionId, 
	X.StateAbbreviation, 
	X.IsStateUsed, 
	X.StateNumber 
	FROM
	WB_IM_State X
	inner join
	cte_WBIMState Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXPTRANS AS (
	SELECT
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	WB_IM_LineId,
	WB_IM_StateId,
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
	StateNumber
	FROM SQ_WB_IM_State
),
WBIMStateStage AS (
	TRUNCATE TABLE WBIMStateStage;
	INSERT INTO WBIMStateStage
	(ExtractDate, SourceSystemId, WBIMLineId, WBIMStateId, SessionId, StateAbbreviation, IsStateUsed, StateNumber)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	WB_IM_LineId AS WBIMLINEID, 
	WB_IM_StateId AS WBIMSTATEID, 
	SESSIONID, 
	STATEABBREVIATION, 
	o_IsStateUsed AS ISSTATEUSED, 
	STATENUMBER
	FROM EXPTRANS
),