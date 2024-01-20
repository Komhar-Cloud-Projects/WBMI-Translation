WITH
SQ_DCCALineStaging AS (
	SELECT
		DCCALineStagingId,
		ExtractDate,
		SourceSystemId,
		LineId,
		CA_LineId,
		SessionId,
		Id,
		Type,
		AttorneysFees,
		CommercialDrivingSchoolTerritory,
		CompositeRating,
		CompositeRatingBasis,
		Description,
		DescriptionOverride,
		DriverTraining,
		GrossReceiptsPIP,
		HiredLiability,
		HiredLiabilityTruckingMotorCarrier,
		HiredPhysicalDamage,
		HiredPhysicalDamageWithDriver,
		LeasingGrossReceipts,
		NOHAFuneralDirectorsMedical,
		NonOwnedAuto,
		PrimaryRateTerritory,
		PublicGrossReceipts,
		TerrorismProgramYear,
		TruckersGrossReceipts
	FROM DCCALineStaging
),
EXP_Metadata AS (
	SELECT
	DCCALineStagingId,
	ExtractDate,
	SourceSystemId,
	LineId,
	CA_LineId,
	SessionId,
	Id,
	Type,
	AttorneysFees,
	CommercialDrivingSchoolTerritory,
	CompositeRating,
	CompositeRatingBasis,
	Description,
	DescriptionOverride,
	DriverTraining,
	GrossReceiptsPIP,
	HiredLiability,
	HiredLiabilityTruckingMotorCarrier,
	HiredPhysicalDamage,
	HiredPhysicalDamageWithDriver,
	LeasingGrossReceipts,
	NOHAFuneralDirectorsMedical,
	NonOwnedAuto,
	PrimaryRateTerritory,
	PublicGrossReceipts,
	TerrorismProgramYear,
	TruckersGrossReceipts,
	-- *INF*: DECODE(AttorneysFees, 'T', 1, 'F', 0, NULL)
	DECODE(
	    AttorneysFees,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_AttorneysFees,
	-- *INF*: DECODE(CompositeRating, 'T', 1, 'F', 0, NULL)
	DECODE(
	    CompositeRating,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_CompositeRating,
	-- *INF*: DECODE(DriverTraining, 'T', 1, 'F', 0, NULL)
	DECODE(
	    DriverTraining,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_DriverTraining,
	-- *INF*: DECODE(GrossReceiptsPIP, 'T', 1, 'F', 0, NULL)
	DECODE(
	    GrossReceiptsPIP,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_GrossReceiptsPIP,
	-- *INF*: DECODE(HiredLiability, 'T', 1, 'F', 0, NULL)
	DECODE(
	    HiredLiability,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_HiredLiability,
	-- *INF*: DECODE(HiredLiabilityTruckingMotorCarrier, 'T', 1, 'F', 0, NULL)
	DECODE(
	    HiredLiabilityTruckingMotorCarrier,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_HiredLiabilityTruckingMotorCarrier,
	-- *INF*: DECODE(HiredPhysicalDamage, 'T', 1, 'F', 0, NULL)
	DECODE(
	    HiredPhysicalDamage,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_HiredPhysicalDamage,
	-- *INF*: DECODE(HiredPhysicalDamageWithDriver, 'T', 1, 'F', 0, NULL)
	DECODE(
	    HiredPhysicalDamageWithDriver,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_HiredPhysicalDamageWithDriver,
	-- *INF*: DECODE(LeasingGrossReceipts, 'T', 1, 'F', 0, NULL)
	DECODE(
	    LeasingGrossReceipts,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_LeasingGrossReceipts,
	-- *INF*: DECODE(NOHAFuneralDirectorsMedical, 'T', 1, 'F', 0, NULL)
	DECODE(
	    NOHAFuneralDirectorsMedical,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_NOHAFuneralDirectorsMedical,
	-- *INF*: DECODE(NonOwnedAuto, 'T', 1, 'F', 0, NULL)
	DECODE(
	    NonOwnedAuto,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_NonOwnedAuto,
	-- *INF*: DECODE(PublicGrossReceipts, 'T', 1, 'F', 0, NULL)
	DECODE(
	    PublicGrossReceipts,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_PublicGrossReceipts,
	-- *INF*: DECODE(TruckersGrossReceipts, 'T', 1, 'F', 0, NULL)
	DECODE(
	    TruckersGrossReceipts,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_TruckersGrossReceipts,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_auditid
	FROM SQ_DCCALineStaging
),
ArchDCCALineStaging AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ArchDCCALineStaging
	(ExtractDate, SourceSystemId, AuditId, LineId, CA_LineId, SessionId, Id, Type, AttorneysFees, CommercialDrivingSchoolTerritory, CompositeRating, CompositeRatingBasis, Description, DescriptionOverride, DriverTraining, GrossReceiptsPIP, HiredLiability, HiredLiabilityTruckingMotorCarrier, HiredPhysicalDamage, HiredPhysicalDamageWithDriver, LeasingGrossReceipts, NOHAFuneralDirectorsMedical, NonOwnedAuto, PrimaryRateTerritory, PublicGrossReceipts, TerrorismProgramYear, TruckersGrossReceipts)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_auditid AS AUDITID, 
	LINEID, 
	CA_LINEID, 
	SESSIONID, 
	ID, 
	TYPE, 
	o_AttorneysFees AS ATTORNEYSFEES, 
	COMMERCIALDRIVINGSCHOOLTERRITORY, 
	o_CompositeRating AS COMPOSITERATING, 
	COMPOSITERATINGBASIS, 
	DESCRIPTION, 
	DESCRIPTIONOVERRIDE, 
	o_DriverTraining AS DRIVERTRAINING, 
	o_GrossReceiptsPIP AS GROSSRECEIPTSPIP, 
	o_HiredLiability AS HIREDLIABILITY, 
	o_HiredLiabilityTruckingMotorCarrier AS HIREDLIABILITYTRUCKINGMOTORCARRIER, 
	o_HiredPhysicalDamage AS HIREDPHYSICALDAMAGE, 
	o_HiredPhysicalDamageWithDriver AS HIREDPHYSICALDAMAGEWITHDRIVER, 
	o_LeasingGrossReceipts AS LEASINGGROSSRECEIPTS, 
	o_NOHAFuneralDirectorsMedical AS NOHAFUNERALDIRECTORSMEDICAL, 
	o_NonOwnedAuto AS NONOWNEDAUTO, 
	PRIMARYRATETERRITORY, 
	o_PublicGrossReceipts AS PUBLICGROSSRECEIPTS, 
	TERRORISMPROGRAMYEAR, 
	o_TruckersGrossReceipts AS TRUCKERSGROSSRECEIPTS
	FROM EXP_Metadata
),