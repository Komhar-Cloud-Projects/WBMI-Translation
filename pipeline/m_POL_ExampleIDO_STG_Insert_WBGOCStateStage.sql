WITH
SQ_WB_GOC_State AS (
	WITH cte_WBGOCState(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.LineId, 
	X.WB_GOC_StateId, 
	X.SessionId, 
	X.IsStateUsed, 
	X.StateAbbreviation, 
	X.StateNumber 
	FROM
	WB_GOC_State X
	inner join
	cte_WBGOCState Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	LineId,
	WB_GOC_StateId,
	SessionId,
	IsStateUsed,
	StateAbbreviation,
	StateNumber
	FROM SQ_WB_GOC_State
),
WBGOCStateStage AS (
	TRUNCATE TABLE WBGOCStateStage;
	INSERT INTO WBGOCStateStage
	(ExtractDate, SourceSystemId, LineId, WBGOCStateId, SessionId, IsStateUsed, StateAbbreviation, StateNumber)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	LINEID, 
	WB_GOC_StateId AS WBGOCSTATEID, 
	SESSIONID, 
	ISSTATEUSED, 
	STATEABBREVIATION, 
	STATENUMBER
	FROM EXP_Metadata
),