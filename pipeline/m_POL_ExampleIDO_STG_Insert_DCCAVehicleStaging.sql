WITH
SQ_DC_CA_Vehicle AS (
	WITH cte_DCCAVehicle(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.CA_RiskId
	,X.CA_VehicleId
	,X.SessionId
	,X.Id
	,X.AgeGroup
	,X.Auditable
	,X.Make
	,X.Model
	,X.NumberOfVehiclesEstimate
	,X.RadiusOfOperation
	,X.RadiusRating
	,X.StatedAmount
	,X.Territory
	,X.[Use]
	,X.VIN
	,X.Year
	,X.ZoneGaraging
	,X.ZoneRating
	,X.ZoneTerminal
	FROM DC_CA_Vehicle X
	inner join
	cte_DCCAVehicle Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	CA_RiskId,
	CA_VehicleId,
	SessionId,
	Id,
	AgeGroup,
	Auditable,
	Make,
	Model,
	NumberOfVehiclesEstimate,
	RadiusOfOperation,
	RadiusRating,
	StatedAmount,
	Territory,
	Use,
	VIN,
	Year,
	ZoneGaraging,
	ZoneRating,
	ZoneTerminal,
	-- *INF*: DECODE(RadiusRating, 'T', 1, 'F', 0, NULL)
	DECODE(
	    RadiusRating,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_RadiusRating,
	-- *INF*: DECODE(ZoneRating, 'T', 1, 'F', 0, NULL)
	DECODE(
	    ZoneRating,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_ZoneRating,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_DC_CA_Vehicle
),
DCCAVehicleStaging AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.DCCAVehicleStaging;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.DCCAVehicleStaging
	(ExtractDate, SourceSystemId, CA_RiskId, CA_VehicleId, SessionId, Id, AgeGroup, Auditable, Make, Model, NumberOfVehiclesEstimate, RadiusOfOperation, RadiusRating, StatedAmount, Territory, Use, VIN, Year, ZoneGaraging, ZoneRating, ZoneTerminal)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	CA_RISKID, 
	CA_VEHICLEID, 
	SESSIONID, 
	ID, 
	AGEGROUP, 
	AUDITABLE, 
	MAKE, 
	MODEL, 
	NUMBEROFVEHICLESESTIMATE, 
	RADIUSOFOPERATION, 
	o_RadiusRating AS RADIUSRATING, 
	STATEDAMOUNT, 
	TERRITORY, 
	USE, 
	VIN, 
	YEAR, 
	ZONEGARAGING, 
	o_ZoneRating AS ZONERATING, 
	ZONETERMINAL
	FROM EXP_Metadata
),