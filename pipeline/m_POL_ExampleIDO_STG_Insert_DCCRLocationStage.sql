WITH
SQ_DC_CR_Location AS (
	WITH cte_DCCRLocation(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.CR_LocationId, 
	X.SessionId, 
	X.Id, 
	X.Number, 
	X.Description, 
	X.TerritoryGroup, 
	X.RatableEmployees 
	FROM
	DC_CR_Location X
	inner join
	cte_DCCRLocation Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	CR_LocationId,
	SessionId,
	Id,
	Number,
	Description,
	TerritoryGroup,
	RatableEmployees,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_DC_CR_Location
),
DCCRLocationStage AS (
	TRUNCATE TABLE DCCRLocationStage;
	INSERT INTO DCCRLocationStage
	(CR_LocationId, SessionId, Id, Number, Description, TerritoryGroup, RatableEmployees, ExtractDate, SourceSystemId)
	SELECT 
	CR_LOCATIONID, 
	SESSIONID, 
	ID, 
	NUMBER, 
	DESCRIPTION, 
	TERRITORYGROUP, 
	RATABLEEMPLOYEES, 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID
	FROM EXP_Metadata
),