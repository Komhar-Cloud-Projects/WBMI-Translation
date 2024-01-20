WITH
SQ_DC_CR_Occupancy AS (
	WITH cte_DCCROccupancy(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.CR_OccupancyId, 
	X.SessionId, 
	X.Id, 
	X.Deleted, 
	X.CrimeClass, 
	X.OccupancyTypeMonoline, 
	X.Description, 
	X.ShortDescription, 
	X.RateGroup, 
	X.RateGroupOverride
	FROM
	DC_CR_Occupancy X
	inner join
	cte_DCCROccupancy Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	CR_OccupancyId,
	SessionId,
	Id,
	Deleted,
	CrimeClass,
	OccupancyTypeMonoline,
	Description,
	ShortDescription,
	RateGroup,
	RateGroupOverride,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_DC_CR_Occupancy
),
DCCROccupancyStage AS (
	TRUNCATE TABLE DCCROccupancyStage;
	INSERT INTO DCCROccupancyStage
	(CR_OccupancyId, SessionId, Id, CrimeClass, OccupancyTypeMonoline, Description, ShortDescription, RateGroup, RateGroupOverride, ExtractDate, SourceSystemId)
	SELECT 
	CR_OCCUPANCYID, 
	SESSIONID, 
	ID, 
	CRIMECLASS, 
	OCCUPANCYTYPEMONOLINE, 
	DESCRIPTION, 
	SHORTDESCRIPTION, 
	RATEGROUP, 
	RATEGROUPOVERRIDE, 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID
	FROM EXP_Metadata
),