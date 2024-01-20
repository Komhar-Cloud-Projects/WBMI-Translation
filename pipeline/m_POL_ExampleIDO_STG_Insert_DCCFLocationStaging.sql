WITH
SQ_DC_CF_Location AS (
	WITH cte_DCCFLocation(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.CF_LocationId, 
	X.SessionId, 
	X.Id, 
	X.Description, 
	X.Number 
	FROM
	DC_CF_Location X
	inner join
	cte_DCCFLocation Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	CF_LocationId,
	SessionId,
	Id,
	Description,
	Number,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_DC_CF_Location
),
DCCFLocationStaging AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.DCCFLocationStaging;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.DCCFLocationStaging
	(CF_LocationId, SessionId, Id, Description, Number, ExtractDate, SourceSystemId)
	SELECT 
	CF_LOCATIONID, 
	SESSIONID, 
	ID, 
	DESCRIPTION, 
	NUMBER, 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID
	FROM EXP_Metadata
),