WITH
SQ_DCCATruckStaging AS (
	SELECT
		DCCATruckStagingId,
		ExtractDate,
		SourceSystemId,
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
		UsedInDumping
	FROM DCCATruckStaging
),
EXP_Metadata AS (
	SELECT
	DCCATruckStagingId,
	ExtractDate,
	SourceSystemId,
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
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM SQ_DCCATruckStaging
),
ArchDCCATruckStaging AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ArchDCCATruckStaging
	(ExtractDate, SourceSystemId, AuditId, CA_VehicleId, CA_TruckId, SessionId, Id, GCW, GVW, HoldHarmlessAgreement, InsuredType, LongTermRentalContracts, MetropolitanZones, NumberOfDaysInsured, NumberOfTrailers, NumberOfTrailersAudit, NumberOfTrailersCalc, SecondaryClassCategory, TruckersSpecialProvisions, UsedInDumping)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
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