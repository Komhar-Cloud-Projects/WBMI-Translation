WITH
SQ_DC_CA_Truck AS (
	WITH cte_DCCATruck(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.CA_VehicleId, 
	X.CA_TruckId, 
	X.SessionId, 
	X.Id, 
	X.GCW, 
	X.GVW, 
	X.HoldHarmlessAgreement, 
	X.InsuredType, 
	X.LongTermRentalContracts, 
	X.MetropolitanZones, 
	X.NumberOfDaysInsured, 
	X.NumberOfTrailers, 
	X.NumberOfTrailersAudit, 
	X.NumberOfTrailersCalc, 
	X.SecondaryClassCategory, 
	X.TruckersSpecialProvisions, 
	X.UsedInDumping 
	FROM
	DC_CA_Truck X
	inner join
	cte_DCCATruck Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	CA_VehicleId,
	CA_TruckId,
	SessionId,
	Id,
	GCW,
	GVW,
	HoldHarmlessAgreement,
	InsuredType,
	LongTermRentalContracts,
	MetropolitanZones,
	NumberOfDaysInsured,
	NumberOfTrailers,
	NumberOfTrailersAudit,
	NumberOfTrailersCalc,
	SecondaryClassCategory,
	TruckersSpecialProvisions,
	UsedInDumping,
	-- *INF*: DECODE(HoldHarmlessAgreement, 'T', 1, 'F', 0, NULL)
	DECODE(
	    HoldHarmlessAgreement,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_HoldHarmlessAgreement,
	-- *INF*: DECODE(LongTermRentalContracts, 'T', 1, 'F', 0, NULL)
	DECODE(
	    LongTermRentalContracts,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_LongTermRentalContracts,
	-- *INF*: DECODE(MetropolitanZones, 'T', 1, 'F', 0, NULL)
	DECODE(
	    MetropolitanZones,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_MetropolitanZones,
	-- *INF*: DECODE(UsedInDumping, 'T', 1, 'F', 0, NULL)
	DECODE(
	    UsedInDumping,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_UsedInDumping,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_DC_CA_Truck
),
DCCATruckStaging AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.DCCATruckStaging;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.DCCATruckStaging
	(ExtractDate, SourceSystemId, CA_VehicleId, CA_TruckId, SessionId, Id, GCW, GVW, HoldHarmlessAgreement, InsuredType, LongTermRentalContracts, MetropolitanZones, NumberOfDaysInsured, NumberOfTrailers, NumberOfTrailersAudit, NumberOfTrailersCalc, SecondaryClassCategory, TruckersSpecialProvisions, UsedInDumping)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	CA_VEHICLEID, 
	CA_TRUCKID, 
	SESSIONID, 
	ID, 
	GCW, 
	GVW, 
	o_HoldHarmlessAgreement AS HOLDHARMLESSAGREEMENT, 
	INSUREDTYPE, 
	o_LongTermRentalContracts AS LONGTERMRENTALCONTRACTS, 
	o_MetropolitanZones AS METROPOLITANZONES, 
	NUMBEROFDAYSINSURED, 
	NUMBEROFTRAILERS, 
	NUMBEROFTRAILERSAUDIT, 
	NUMBEROFTRAILERSCALC, 
	SECONDARYCLASSCATEGORY, 
	TRUCKERSSPECIALPROVISIONS, 
	o_UsedInDumping AS USEDINDUMPING
	FROM EXP_Metadata
),