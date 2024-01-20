WITH
SQ_DC_CA_State AS (
	WITH cte_DCCAState(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.LineId, 
	X.CA_StateId, 
	X.SessionId, 
	X.Id, 
	X.Deleted, 
	X.Description, 
	X.DriveOtherCarCoverage, 
	X.EquipmentExcessiveCoverage, 
	X.FleetReductionSelection, 
	X.HiredLiability, 
	X.HiredLiabilityTrickingMotorCarrier, 
	X.HiredPhysicalDamage, 
	X.HiredPhysicalDamageWithDriver, 
	X.LineCoverageState, 
	X.MotorJunkLicenseCoverage, 
	X.NumberFamilyMembers, 
	X.OfficialInspectionStation, 
	X.RentalVehicleLiabilityCoverage, 
	X.SubjectToNoFault, 
	X.TXPremiumDiscount, 
	X.VehiclesMovedUnderWritOfAttachmentCoverage,
	X.NumberOfEmployees   
	FROM
	DC_CA_State X
	inner join
	cte_DCCAState Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	LineId,
	CA_StateId,
	SessionId,
	Id,
	Deleted,
	Description,
	DriveOtherCarCoverage,
	EquipmentExcessiveCoverage,
	FleetReductionSelection,
	HiredLiability,
	HiredLiabilityTrickingMotorCarrier,
	HiredPhysicalDamage,
	HiredPhysicalDamageWithDriver,
	LineCoverageState,
	MotorJunkLicenseCoverage,
	NumberFamilyMembers,
	OfficialInspectionStation,
	RentalVehicleLiabilityCoverage,
	SubjectToNoFault,
	TXPremiumDiscount,
	VehiclesMovedUnderWritOfAttachmentCoverage,
	-- *INF*: DECODE(Deleted, 'T', 1, 'F', 0, NULL)
	DECODE(
	    Deleted,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_Deleted,
	-- *INF*: DECODE(DriveOtherCarCoverage, 'T', 1, 'F', 0, NULL)
	DECODE(
	    DriveOtherCarCoverage,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_DriveOtherCarCoverage,
	-- *INF*: DECODE(EquipmentExcessiveCoverage, 'T', 1, 'F', 0, NULL)
	DECODE(
	    EquipmentExcessiveCoverage,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_EquipmentExcessiveCoverage,
	-- *INF*: DECODE(FleetReductionSelection, 'T', 1, 'F', 0, NULL)
	DECODE(
	    FleetReductionSelection,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_FleetReductionSelection,
	-- *INF*: DECODE(HiredLiability, 'T', 1, 'F', 0, NULL)
	DECODE(
	    HiredLiability,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_HiredLiability,
	-- *INF*: DECODE(HiredLiabilityTrickingMotorCarrier, 'T', 1, 'F', 0, NULL)
	DECODE(
	    HiredLiabilityTrickingMotorCarrier,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_HiredLiabilityTrickingMotorCarrier,
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
	-- *INF*: DECODE(MotorJunkLicenseCoverage, 'T', 1, 'F', 0, NULL)
	DECODE(
	    MotorJunkLicenseCoverage,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_MotorJunkLicenseCoverage,
	-- *INF*: DECODE(OfficialInspectionStation, 'T', 1, 'F', 0, NULL)
	DECODE(
	    OfficialInspectionStation,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_OfficialInspectionStation,
	-- *INF*: DECODE(RentalVehicleLiabilityCoverage, 'T', 1, 'F', 0, NULL)
	DECODE(
	    RentalVehicleLiabilityCoverage,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_RentalVehicleLiabilityCoverage,
	-- *INF*: DECODE(SubjectToNoFault, 'T', 1, 'F', 0, NULL)
	DECODE(
	    SubjectToNoFault,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_SubjectToNoFault,
	-- *INF*: DECODE(TXPremiumDiscount, 'T', 1, 'F', 0, NULL)
	DECODE(
	    TXPremiumDiscount,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_TXPremiumDiscount,
	-- *INF*: DECODE(VehiclesMovedUnderWritOfAttachmentCoverage, 'T', 1, 'F', 0, NULL)
	DECODE(
	    VehiclesMovedUnderWritOfAttachmentCoverage,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_VehiclesMovedUnderWritOfAttachmentCoverage,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	NumberOfEmployees
	FROM SQ_DC_CA_State
),
DCCAStateStaging AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.DCCAStateStaging;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.DCCAStateStaging
	(ExtractDate, SourceSystemId, LineId, CA_StateId, SessionId, Id, Description, DriveOtherCarCoverage, EquipmentExcessiveCoverage, FleetReductionSelection, HiredLiability, HiredLiabilityTrickingMotorCarrier, HiredPhysicalDamage, HiredPhysicalDamageWithDriver, LineCoverageState, MotorJunkLicenseCoverage, NumberFamilyMembers, OfficialInspectionStation, RentalVehicleLiabilityCoverage, SubjectToNoFault, TXPremiumDiscount, VehiclesMovedUnderWritOfAttachmentCoverage, Deleted, NumberOfEmployees)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	LINEID, 
	CA_STATEID, 
	SESSIONID, 
	ID, 
	DESCRIPTION, 
	o_DriveOtherCarCoverage AS DRIVEOTHERCARCOVERAGE, 
	o_EquipmentExcessiveCoverage AS EQUIPMENTEXCESSIVECOVERAGE, 
	o_FleetReductionSelection AS FLEETREDUCTIONSELECTION, 
	o_HiredLiability AS HIREDLIABILITY, 
	o_HiredLiabilityTrickingMotorCarrier AS HIREDLIABILITYTRICKINGMOTORCARRIER, 
	o_HiredPhysicalDamage AS HIREDPHYSICALDAMAGE, 
	o_HiredPhysicalDamageWithDriver AS HIREDPHYSICALDAMAGEWITHDRIVER, 
	LINECOVERAGESTATE, 
	o_MotorJunkLicenseCoverage AS MOTORJUNKLICENSECOVERAGE, 
	NUMBERFAMILYMEMBERS, 
	o_OfficialInspectionStation AS OFFICIALINSPECTIONSTATION, 
	o_RentalVehicleLiabilityCoverage AS RENTALVEHICLELIABILITYCOVERAGE, 
	o_SubjectToNoFault AS SUBJECTTONOFAULT, 
	o_TXPremiumDiscount AS TXPREMIUMDISCOUNT, 
	o_VehiclesMovedUnderWritOfAttachmentCoverage AS VEHICLESMOVEDUNDERWRITOFATTACHMENTCOVERAGE, 
	o_Deleted AS DELETED, 
	NUMBEROFEMPLOYEES
	FROM EXP_Metadata
),