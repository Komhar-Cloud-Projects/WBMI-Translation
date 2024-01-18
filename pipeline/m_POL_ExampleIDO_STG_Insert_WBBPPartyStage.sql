WITH
SQ_WB_BP_Party AS (
	WITH cte_WBBPParty(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.WB_CL_PartyId, 
	X.WB_BP_PartyId, 
	X.SessionId, 
	X.NumberOfUnits, 
	X.NumberOfPools, 
	X.AnyDryCleaning, 
	X.AnySaleOfAnimals, 
	X.AnySaleOfTravelTickets, 
	X.TaxidermyWork, 
	X.InstallationServiceOrRepair, 
	X.BOP_New_BusinessClassCode, 
	X.BOP_New_BusinessSegment, 
	X.BOP_New_COBLiabilityFactor, 
	X.BOP_New_COBPropertyFactor, 
	X.BOP_New_COBAllOtherFactor, 
	X.LocationNumber, 
	X.BuildingNumber, 
	X.BuildingDescription, 
	X.ConstructionCode, 
	X.LocationProtectionClass, 
	X.BOP_New_EquipmentBreakdownGroup, 
	X.SelectLocationForCopy, 
	X.SelectBuildingValidRefTest 
	FROM
	WB_BP_Party X
	inner join
	cte_WBBPParty Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	WB_CL_PartyId,
	WB_BP_PartyId,
	SessionId,
	NumberOfUnits,
	NumberOfPools,
	AnyDryCleaning,
	-- *INF*: DECODE(AnyDryCleaning,'T','1','F','0')
	DECODE(
	    AnyDryCleaning,
	    'T', '1',
	    'F', '0'
	) AS o_AnyDryCleaning,
	AnySaleOfAnimals,
	-- *INF*: DECODE(AnySaleOfAnimals,'T','1','F','0')
	DECODE(
	    AnySaleOfAnimals,
	    'T', '1',
	    'F', '0'
	) AS o_AnySaleOfAnimals,
	AnySaleOfTravelTickets,
	-- *INF*: DECODE(AnySaleOfTravelTickets,'T','1','F','0')
	-- 
	DECODE(
	    AnySaleOfTravelTickets,
	    'T', '1',
	    'F', '0'
	) AS o_AnySaleOfTravelTickets,
	TaxidermyWork,
	-- *INF*: DECODE(TaxidermyWork,'T','1','F','0')
	DECODE(
	    TaxidermyWork,
	    'T', '1',
	    'F', '0'
	) AS o_TaxidermyWork,
	InstallationServiceOrRepair,
	-- *INF*: DECODE(InstallationServiceOrRepair,'T','1','F','0')
	DECODE(
	    InstallationServiceOrRepair,
	    'T', '1',
	    'F', '0'
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
	FROM SQ_WB_BP_Party
),
WBBPPartyStage AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.WBBPPartyStage;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.WBBPPartyStage
	(ExtractDate, SourceSystemId, WB_CL_PartyId, WB_BP_PartyId, SessionId, NumberOfUnits, NumberOfPools, AnyDryCleaning, AnySaleOfAnimals, AnySaleOfTravelTickets, TaxidermyWork, InstallationServiceOrRepair, BOP_New_BusinessClassCode, BOP_New_BusinessSegment, BOP_New_COBLiabilityFactor, BOP_New_COBPropertyFactor, BOP_New_COBAllOtherFactor, LocationNumber, BuildingNumber, BuildingDescription, ConstructionCode, LocationProtectionClass, BOP_New_EquipmentBreakdownGroup, SelectLocationForCopy, SelectBuildingValidRefTest)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
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
	FROM EXP_Metadata
),