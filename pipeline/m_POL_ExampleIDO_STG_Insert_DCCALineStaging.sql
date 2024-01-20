WITH
SQ_DC_CA_Line AS (
	WITH cte_DCCALine(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.LineId, 
	X.CA_LineId, 
	X.SessionId, 
	X.Id, 
	X.Type, 
	X.AttorneysFees, 
	X.CommercialDrivingSchoolTerritory, 
	X.CompositeRating, 
	X.CompositeRatingBasis, 
	X.Description, 
	X.DescriptionOverride, 
	X.DriverTraining, 
	X.GrossReceiptsPIP, 
	X.HiredLiability, 
	X.HiredLiabilityTruckingMotorCarrier, 
	X.HiredPhysicalDamage, 
	X.HiredPhysicalDamageWithDriver, 
	X.LeasingGrossReceipts, 
	X.NOHAFuneralDirectorsMedical, 
	X.NonOwnedAuto, 
	X.PrimaryRateTerritory, 
	X.PublicGrossReceipts, 
	X.TerrorismProgramYear, 
	X.TruckersGrossReceipts 
	FROM
	DC_CA_Line X
	inner join
	cte_DCCALine Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
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
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_DC_CA_Line
),
DCCALineStaging AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.DCCALineStaging;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.DCCALineStaging
	(ExtractDate, SourceSystemId, LineId, CA_LineId, SessionId, Id, Type, AttorneysFees, CommercialDrivingSchoolTerritory, CompositeRating, CompositeRatingBasis, Description, DescriptionOverride, DriverTraining, GrossReceiptsPIP, HiredLiability, HiredLiabilityTruckingMotorCarrier, HiredPhysicalDamage, HiredPhysicalDamageWithDriver, LeasingGrossReceipts, NOHAFuneralDirectorsMedical, NonOwnedAuto, PrimaryRateTerritory, PublicGrossReceipts, TerrorismProgramYear, TruckersGrossReceipts)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
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