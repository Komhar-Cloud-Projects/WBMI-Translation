WITH
SQ_WBBPPartyStaging AS (
	SELECT
		WBBPPartyStageId AS WBBPPartyStageID,
		ExtractDate,
		SourceSystemId,
		WB_CL_PartyId,
		WB_BP_PartyId,
		SessionId,
		NumberOfUnits,
		NumberOfPools,
		AnyDryCleaning,
		AnySaleOfAnimals,
		AnySaleOfTravelTickets,
		TaxidermyWork,
		InstallationServiceOrRepair,
		BOP_New_BusinessClassCode,
		BOP_New_BusinessSegment,
		BOP_New_COBLiabilityFactor,
		BOP_New_COBPropertyFactor,
		BOP_New_COBAllOtherFactor,
		LocationNumber,
		BuildingNumber,
		BuildingDescription,
		ConstructionCode,
		LocationProtectionClass,
		BOP_New_EquipmentBreakdownGroup,
		SelectLocationForCopy,
		SelectBuildingValidRefTest
	FROM WBBPPartyStaging
),
EXPTRANS AS (
	SELECT
	WBBPPartyStageID,
	ExtractDate,
	SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	WB_CL_PartyId,
	WB_BP_PartyId,
	SessionId,
	NumberOfUnits,
	NumberOfPools,
	AnyDryCleaning,
	-- *INF*: DECODE(AnyDryCleaning, 'T', '1', 'F', '0', NULL)
	DECODE(
	    AnyDryCleaning,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_AnyDryCleaning,
	AnySaleOfAnimals,
	-- *INF*: DECODE(AnySaleOfAnimals, 'T', '1', 'F', '0', NULL)
	DECODE(
	    AnySaleOfAnimals,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_AnySaleOfAnimals,
	AnySaleOfTravelTickets,
	-- *INF*: DECODE(AnySaleOfTravelTickets, 'T', '1', 'F', '0', NULL)
	DECODE(
	    AnySaleOfTravelTickets,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_AnySaleOfTravelTickets,
	TaxidermyWork,
	-- *INF*: DECODE(TaxidermyWork, 'T', '1', 'F', '0', NULL)
	DECODE(
	    TaxidermyWork,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_TaxidermyWork,
	InstallationServiceOrRepair,
	-- *INF*: DECODE(InstallationServiceOrRepair, 'T', '1', 'F', '0', NULL)
	DECODE(
	    InstallationServiceOrRepair,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_InstallationServiceOrRepair,
	BOP_New_BusinessClassCode,
	BOP_New_BusinessSegment,
	BOP_New_COBLiabilityFactor,
	BOP_New_COBPropertyFactor,
	BOP_New_COBAllOtherFactor,
	LocationNumber,
	BuildingNumber,
	BuildingDescription,
	ConstructionCode,
	LocationProtectionClass,
	BOP_New_EquipmentBreakdownGroup,
	SelectLocationForCopy,
	SelectBuildingValidRefTest
	FROM SQ_WBBPPartyStaging
),
ArchWBBPPartyStaging AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ArchWBBPPartyStage
	(ExtractDate, SourceSystemId, AuditId, WBBPPartyStageId, WB_CL_PartyId, WB_BP_PartyId, SessionId, NumberOfUnits, NumberOfPools, AnyDryCleaning, AnySaleOfAnimals, AnySaleOfTravelTickets, TaxidermyWork, InstallationServiceOrRepair, BOP_New_BusinessClassCode, BOP_New_BusinessSegment, BOP_New_COBLiabilityFactor, BOP_New_COBPropertyFactor, BOP_New_COBAllOtherFactor, LocationNumber, BuildingNumber, BuildingDescription, ConstructionCode, LocationProtectionClass, BOP_New_EquipmentBreakdownGroup, SelectLocationForCopy, SelectBuildingValidRefTest)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
	WBBPPartyStageID AS WBBPPARTYSTAGEID, 
	WB_CL_PARTYID, 
	WB_BP_PARTYID, 
	SESSIONID, 
	NUMBEROFUNITS, 
	NUMBEROFPOOLS, 
	o_AnyDryCleaning AS ANYDRYCLEANING, 
	o_AnySaleOfAnimals AS ANYSALEOFANIMALS, 
	o_AnySaleOfTravelTickets AS ANYSALEOFTRAVELTICKETS, 
	o_TaxidermyWork AS TAXIDERMYWORK, 
	o_InstallationServiceOrRepair AS INSTALLATIONSERVICEORREPAIR, 
	BOP_NEW_BUSINESSCLASSCODE, 
	BOP_NEW_BUSINESSSEGMENT, 
	BOP_NEW_COBLIABILITYFACTOR, 
	BOP_NEW_COBPROPERTYFACTOR, 
	BOP_NEW_COBALLOTHERFACTOR, 
	LOCATIONNUMBER, 
	BUILDINGNUMBER, 
	BUILDINGDESCRIPTION, 
	CONSTRUCTIONCODE, 
	LOCATIONPROTECTIONCLASS, 
	BOP_NEW_EQUIPMENTBREAKDOWNGROUP, 
	SELECTLOCATIONFORCOPY, 
	SELECTBUILDINGVALIDREFTEST
	FROM EXPTRANS
),