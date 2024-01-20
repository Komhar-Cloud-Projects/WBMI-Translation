WITH
SQ_DCCAStateStaging AS (
	SELECT
		DCCAStateStagingId,
		ExtractDate,
		SourceSystemId,
		LineId,
		CA_StateId,
		SessionId,
		Id,
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
		Deleted,
		NumberOfEmployees
	FROM DCCAStateStaging
),
EXP_Metadata AS (
	SELECT
	DCCAStateStagingId,
	ExtractDate,
	SourceSystemId,
	LineId,
	CA_StateId,
	SessionId,
	Id,
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
	Deleted,
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
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	NumberOfEmployees
	FROM SQ_DCCAStateStaging
),
ArchDCCAStateStaging AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ArchDCCAStateStaging
	(ExtractDate, SourceSystemId, AuditId, LineId, CA_StateId, SessionId, Id, Description, DriveOtherCarCoverage, EquipmentExcessiveCoverage, FleetReductionSelection, HiredLiability, HiredLiabilityTrickingMotorCarrier, HiredPhysicalDamage, HiredPhysicalDamageWithDriver, LineCoverageState, MotorJunkLicenseCoverage, NumberFamilyMembers, OfficialInspectionStation, RentalVehicleLiabilityCoverage, SubjectToNoFault, TXPremiumDiscount, VehiclesMovedUnderWritOfAttachmentCoverage, Deleted, NumberOfEmployees)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
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