WITH
SQ_DC_IM_Item AS (
	WITH cte_DCIMItem(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.CoverageId, 
	X.IM_ItemId, 
	X.SessionId, 
	X.Id, 
	X.Type 
	FROM
	DC_IM_Item X
	inner join
	cte_DCIMItem Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	CoverageId,
	IM_ItemId,
	SessionId,
	Id,
	Type,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_DC_IM_Item
),
DCIMItemStage5 AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.DCIMItemStage;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.DCIMItemStage
	(CoverageId, IMItemId, SessionId, Id, Type, ExtractDate, SourceSystemId)
	SELECT 
	COVERAGEID, 
	IM_ItemId AS IMITEMID, 
	SESSIONID, 
	ID, 
	TYPE, 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID
	FROM EXP_Metadata
),