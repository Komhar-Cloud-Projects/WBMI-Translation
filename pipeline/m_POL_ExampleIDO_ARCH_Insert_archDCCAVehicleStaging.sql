WITH
SQ_DCCAVehicleStaging AS (
	SELECT [DCCAVehicleStagingId]
	      ,[ExtractDate]
	      ,[SourceSystemId]
	      ,[CA_RiskId]
	      ,[CA_VehicleId]
	      ,[SessionId]
	      ,[Id]
	      ,[AgeGroup]
	      ,[Auditable]
	      ,[Make]
	      ,[Model]
	      ,[NumberOfVehiclesEstimate]
	      ,[RadiusOfOperation]
	      ,[RadiusRating]
	      ,[StatedAmount]
	      ,[Territory]
	      ,[Use]
	      ,[VIN]
	      ,[Year]
	      ,[ZoneGaraging]
	      ,[ZoneRating]
	      ,[ZoneTerminal]
	  FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.[DCCAVehicleStaging]
),
EXP_Metadata AS (
	SELECT
	DCCAVehicleStagingId,
	ExtractDate,
	SourceSystemId,
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
	-- *INF*: DECODE(ZoneRating, 'T', 1, 'F', 0, NULL)
	DECODE(
	    ZoneRating,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_ZoneRating,
	-- *INF*: DECODE(RadiusRating, 'T', 1, 'F', 0, NULL)
	DECODE(
	    RadiusRating,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_RadiusRating,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM SQ_DCCAVehicleStaging
),
ArchDCCAVehicleStaging AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ArchDCCAVehicleStaging
	(ExtractDate, SourceSystemId, AuditId, CA_RiskId, CA_VehicleId, SessionId, Id, AgeGroup, Auditable, Make, Model, NumberOfVehiclesEstimate, RadiusOfOperation, RadiusRating, StatedAmount, Territory, Use, VIN, Year, ZoneGaraging, ZoneRating, ZoneTerminal)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
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