WITH
SQ_WB_EC_Line AS (
	WITH cte_WB_EC_Line(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.LineId,
	X.WB_CL_LineId,
	X.WB_EC_LineId,
	X.SessionId 
	FROM
	WB_EC_Line X
	inner join
	cte_WB_EC_Line Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	SYSDATE AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	LineId,
	WB_CL_LineId,
	WB_EC_LineId,
	SessionId
	FROM SQ_WB_EC_Line
),
WBECLineStage AS (
	TRUNCATE TABLE WBECLineStage;
	INSERT INTO WBECLineStage
	(ExtractDate, SourceSystemId, LineId, WB_CL_LineId, WB_EC_LineId, SessionId)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	LINEID, 
	WB_CL_LINEID, 
	WB_EC_LINEID, 
	SESSIONID
	FROM EXP_Metadata
),