WITH
SQ_DCCFBuildingStage AS (
	SELECT
		DCCFBuildingStageId,
		ExtractDate,
		SourceSystemId,
		LineId,
		CFLocationId,
		CFBuildingId,
		SessionId,
		Id,
		Deleted,
		AgreedValue,
		ApplicableCountStored,
		Apply,
		BG1SpecificOccupancyGroupLA,
		BG2Masonry,
		BG2Rise,
		BG2Steel,
		BG2Symbol,
		BG2SymbolPrefix,
		BG2TerritoryHI,
		BlanketCoinsurance,
		BrandsAndLabels,
		BuildingCodeEffectivenessGrading,
		BuildingGroup,
		BuildingType,
		BurglarAlarm,
		ClassLimit,
		Composite,
		Condominium,
		ConstructionCode,
		ContentsHighLimitStored,
		Conversion,
		CSP,
		DCApartmentOption,
		DCOfficeOption,
		DebrisRemoval,
		Deductible,
		Description,
		DesignCode,
		DesignExposure,
		DoorType,
		EarthquakeBuildingClass,
		Exposures,
		ExteriorWalls,
		FunctionalValuation,
		GreenUpgrades,
		GreenUpgradesPeriodOfRestoration,
		HazardousConditions,
		HeatingCooking,
		Housekeeping,
		InternalPressureDesign,
		IsDwelling,
		ManufacturersPersonalProperty,
		ManufacturersStock,
		MineSubLossAssessCondo,
		MultipleOccupancies,
		MultipleOccupanciesClassCode,
		NewResidentialConstructionCredit,
		NumberOfStories,
		OpeningProtection,
		OtherProtectiveDevice,
		PhysicalCondition,
		PhysicalConditionWA,
		PierOrWharf,
		PierOrWharfCauseOfLoss,
		PremiumInsulator,
		RoofCovering,
		RoofCoveringSC,
		RoofDeck,
		RoofDeckAttachment,
		RoofGeometry,
		RoofShape,
		RoofWallConnection,
		RoofWallConstruction,
		SCArea,
		SecondaryWaterResistance,
		Sprinkler,
		SprinklerLeakageExclude,
		SquareFt,
		SubstandardCondition,
		TenantRelocationExpenseEndorsementApplicable,
		Terrain,
		UnderConstruction,
		UtilityServicesDirectDamage,
		VacancyPermit,
		VacancyPermitExcludeSprinklerLeakage,
		VacancyPermitExcludeVandalism,
		VacantBuilding,
		VandalismExclude,
		WatchmanProtection,
		WindBorneDebrisRegion,
		WindHailExcludeSelect,
		WindowProtection,
		WindSpeedDesignSpeed,
		WindSpeedGustWindSpeedOfDesign,
		WindSpeedGustWindSpeedOfLocation,
		WindstormLossMitigation,
		Wiring,
		YearBuilt,
		HazardousSubstanceARate,
		CFLocationXmlId,
		LocationBuildingNumber
	FROM DCCFBuildingStage
),
EXP_Metadata AS (
	SELECT
	DCCFBuildingStageId,
	LineId,
	CFLocationId,
	CFBuildingId,
	SessionId,
	Id,
	Deleted AS i_Deleted,
	-- *INF*: DECODE(i_Deleted,'T','1','F','0',NULL)
	DECODE(
	    i_Deleted,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_Deleted,
	AgreedValue,
	ApplicableCountStored,
	Apply AS i_Apply,
	-- *INF*: DECODE(i_Apply,'T',1,'F',0,NULL)
	DECODE(
	    i_Apply,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_Apply,
	BG1SpecificOccupancyGroupLA,
	BG2Masonry,
	BG2Rise,
	BG2Steel,
	BG2Symbol,
	BG2SymbolPrefix,
	BG2TerritoryHI,
	BlanketCoinsurance,
	BrandsAndLabels,
	BuildingCodeEffectivenessGrading,
	BuildingGroup,
	BuildingType,
	BurglarAlarm,
	ClassLimit,
	Composite,
	Condominium,
	ConstructionCode,
	ContentsHighLimitStored,
	Conversion AS i_Conversion,
	-- *INF*: DECODE(i_Conversion,'T',1,'F',0,NULL)
	DECODE(
	    i_Conversion,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_Conversion,
	CSP,
	DCApartmentOption AS i_DCApartmentOption,
	-- *INF*: DECODE(i_DCApartmentOption,'T',1,'F',0,NULL)
	DECODE(
	    i_DCApartmentOption,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_DCApartmentOption,
	DCOfficeOption AS i_DCOfficeOption,
	-- *INF*: DECODE(i_DCOfficeOption,'T',1,'F',0,NULL)
	DECODE(
	    i_DCOfficeOption,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_DCOfficeOption,
	DebrisRemoval AS i_DebrisRemoval,
	-- *INF*: DECODE(i_DebrisRemoval,'T',1,'F',0,NULL)
	DECODE(
	    i_DebrisRemoval,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_DebrisRemoval,
	Deductible,
	Description,
	DesignCode,
	DesignExposure,
	DoorType,
	EarthquakeBuildingClass,
	Exposures AS i_Exposures,
	-- *INF*: DECODE(i_Exposures,'T',1,'F',0,NULL)
	DECODE(
	    i_Exposures,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_Exposures,
	ExteriorWalls,
	FunctionalValuation,
	GreenUpgrades AS i_GreenUpgrades,
	-- *INF*: DECODE(i_GreenUpgrades,'T',1,'F',0,NULL)
	DECODE(
	    i_GreenUpgrades,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_GreenUpgrades,
	GreenUpgradesPeriodOfRestoration,
	HazardousConditions AS i_HazardousConditions,
	-- *INF*: DECODE(i_HazardousConditions,'T',1,'F',0,NULL)
	DECODE(
	    i_HazardousConditions,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_HazardousConditions,
	HeatingCooking AS i_HeatingCooking,
	-- *INF*: DECODE(i_HeatingCooking,'T',1,'F',0,NULL)
	DECODE(
	    i_HeatingCooking,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_HeatingCooking,
	Housekeeping AS i_Housekeeping,
	-- *INF*: DECODE(i_Housekeeping,'T',1,'F',0,NULL)
	DECODE(
	    i_Housekeeping,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_Housekeeping,
	InternalPressureDesign,
	IsDwelling AS i_IsDwelling,
	-- *INF*: DECODE(i_IsDwelling,'T',1,'F',0,NULL)
	DECODE(
	    i_IsDwelling,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_IsDwelling,
	ManufacturersPersonalProperty,
	ManufacturersStock,
	MineSubLossAssessCondo AS i_MineSubLossAssessCondo,
	-- *INF*: DECODE(i_MineSubLossAssessCondo,'T',1,'F',0,NULL)
	DECODE(
	    i_MineSubLossAssessCondo,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_MineSubLossAssessCondo,
	MultipleOccupancies AS i_MultipleOccupancies,
	-- *INF*: DECODE(i_MultipleOccupancies,'T',1,'F',0,NULL)
	DECODE(
	    i_MultipleOccupancies,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_MultipleOccupancies,
	MultipleOccupanciesClassCode,
	NewResidentialConstructionCredit AS i_NewResidentialConstructionCredit,
	-- *INF*: DECODE(i_NewResidentialConstructionCredit,'T',1,'F',0,NULL)
	DECODE(
	    i_NewResidentialConstructionCredit,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_NewResidentialConstructionCredit,
	NumberOfStories,
	OpeningProtection,
	OtherProtectiveDevice,
	PhysicalCondition AS i_PhysicalCondition,
	-- *INF*: DECODE(i_PhysicalCondition,'T',1,'F',0,NULL)
	DECODE(
	    i_PhysicalCondition,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_PhysicalCondition,
	PhysicalConditionWA AS i_PhysicalConditionWA,
	-- *INF*: DECODE(i_PhysicalConditionWA,'T',1,'F',0,NULL)
	DECODE(
	    i_PhysicalConditionWA,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_PhysicalConditionWA,
	PierOrWharf AS i_PierOrWharf,
	-- *INF*: DECODE(i_PierOrWharf,'T',1,'F',0,NULL)
	DECODE(
	    i_PierOrWharf,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_PierOrWharf,
	PierOrWharfCauseOfLoss,
	PremiumInsulator,
	RoofCovering,
	RoofCoveringSC,
	RoofDeck,
	RoofDeckAttachment,
	RoofGeometry,
	RoofShape,
	RoofWallConnection,
	RoofWallConstruction,
	SCArea,
	SecondaryWaterResistance AS i_SecondaryWaterResistance,
	-- *INF*: DECODE(i_SecondaryWaterResistance,'T',1,'F',0,NULL)
	DECODE(
	    i_SecondaryWaterResistance,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_SecondaryWaterResistance,
	Sprinkler AS i_Sprinkler,
	-- *INF*: DECODE(i_Sprinkler,'T',1,'F',0,NULL)
	DECODE(
	    i_Sprinkler,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_Sprinkler,
	SprinklerLeakageExclude,
	SquareFt,
	SubstandardCondition AS i_SubstandardCondition,
	-- *INF*: DECODE(i_SubstandardCondition,'T',1,'F',0,NULL)
	DECODE(
	    i_SubstandardCondition,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_SubstandardCondition,
	TenantRelocationExpenseEndorsementApplicable AS i_TenantRelocationExpenseEndorsementApplicable,
	-- *INF*: DECODE(i_TenantRelocationExpenseEndorsementApplicable,'T',1,'F',0,NULL)
	DECODE(
	    i_TenantRelocationExpenseEndorsementApplicable,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_TenantRelocationExpenseEndorsementApplicable,
	Terrain,
	UnderConstruction AS i_UnderConstruction,
	-- *INF*: DECODE(i_UnderConstruction,'T',1,'F',0,NULL)
	DECODE(
	    i_UnderConstruction,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_UnderConstruction,
	UtilityServicesDirectDamage,
	VacancyPermit AS i_VacancyPermit,
	-- *INF*: DECODE(i_VacancyPermit,'T',1,'F',0,NULL)
	DECODE(
	    i_VacancyPermit,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_VacancyPermit,
	VacancyPermitExcludeSprinklerLeakage AS i_VacancyPermitExcludeSprinklerLeakage,
	-- *INF*: DECODE(i_VacancyPermitExcludeSprinklerLeakage,'T',1,'F',0,NULL)
	DECODE(
	    i_VacancyPermitExcludeSprinklerLeakage,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_VacancyPermitExcludeSprinklerLeakage,
	VacancyPermitExcludeVandalism AS i_VacancyPermitExcludeVandalism,
	-- *INF*: DECODE(i_VacancyPermitExcludeVandalism,'T',1,'F',0,NULL)
	DECODE(
	    i_VacancyPermitExcludeVandalism,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_VacancyPermitExcludeVandalism,
	VacantBuilding AS i_VacantBuilding,
	-- *INF*: DECODE(i_VacantBuilding,'T',1,'F',0,NULL)
	DECODE(
	    i_VacantBuilding,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_VacantBuilding,
	VandalismExclude AS i_VandalismExclude,
	-- *INF*: DECODE(i_VandalismExclude,'T',1,'F',0,NULL)
	DECODE(
	    i_VandalismExclude,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_VandalismExclude,
	WatchmanProtection,
	WindBorneDebrisRegion AS i_WindBorneDebrisRegion,
	-- *INF*: DECODE(i_WindBorneDebrisRegion,'T',1,'F',0,NULL)
	DECODE(
	    i_WindBorneDebrisRegion,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_WindBorneDebrisRegion,
	WindHailExcludeSelect AS i_WindHailExcludeSelect,
	-- *INF*: DECODE(i_WindHailExcludeSelect,'T',1,'F',0,NULL)
	DECODE(
	    i_WindHailExcludeSelect,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_WindHailExcludeSelect,
	WindowProtection,
	WindSpeedDesignSpeed,
	WindSpeedGustWindSpeedOfDesign,
	WindSpeedGustWindSpeedOfLocation,
	WindstormLossMitigation AS i_WindstormLossMitigation,
	-- *INF*: DECODE(i_WindstormLossMitigation,'T',1,'F',0,NULL)
	DECODE(
	    i_WindstormLossMitigation,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_WindstormLossMitigation,
	Wiring AS i_Wiring,
	-- *INF*: DECODE(i_Wiring,'T',1,'F',0,NULL)
	DECODE(
	    i_Wiring,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_Wiring,
	YearBuilt,
	HazardousSubstanceARate,
	CFLocationXmlId,
	ExtractDate,
	SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	LocationBuildingNumber
	FROM SQ_DCCFBuildingStage
),
ArchDCCFBuildingStage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ArchDCCFBuildingStage
	(ExtractDate, SourceSystemId, AuditId, DCCFBuildingStageId, LineId, CFLocationId, CFBuildingId, SessionId, Id, Deleted, AgreedValue, ApplicableCountStored, Apply, BG1SpecificOccupancyGroupLA, BG2Masonry, BG2Rise, BG2Steel, BG2Symbol, BG2SymbolPrefix, BG2TerritoryHI, BlanketCoinsurance, BrandsAndLabels, BuildingCodeEffectivenessGrading, BuildingGroup, BuildingType, BurglarAlarm, ClassLimit, Composite, Condominium, ConstructionCode, ContentsHighLimitStored, Conversion, CSP, DCApartmentOption, DCOfficeOption, DebrisRemoval, Deductible, Description, DesignCode, DesignExposure, DoorType, EarthquakeBuildingClass, Exposures, ExteriorWalls, FunctionalValuation, GreenUpgrades, GreenUpgradesPeriodOfRestoration, HazardousConditions, HeatingCooking, Housekeeping, InternalPressureDesign, IsDwelling, ManufacturersPersonalProperty, ManufacturersStock, MineSubLossAssessCondo, MultipleOccupancies, MultipleOccupanciesClassCode, NewResidentialConstructionCredit, NumberOfStories, OpeningProtection, OtherProtectiveDevice, PhysicalCondition, PhysicalConditionWA, PierOrWharf, PierOrWharfCauseOfLoss, PremiumInsulator, RoofCovering, RoofCoveringSC, RoofDeck, RoofDeckAttachment, RoofGeometry, RoofShape, RoofWallConnection, RoofWallConstruction, SCArea, SecondaryWaterResistance, Sprinkler, SprinklerLeakageExclude, SquareFt, SubstandardCondition, TenantRelocationExpenseEndorsementApplicable, Terrain, UnderConstruction, UtilityServicesDirectDamage, VacancyPermit, VacancyPermitExcludeSprinklerLeakage, VacancyPermitExcludeVandalism, VacantBuilding, VandalismExclude, WatchmanProtection, WindBorneDebrisRegion, WindHailExcludeSelect, WindowProtection, WindSpeedDesignSpeed, WindSpeedGustWindSpeedOfDesign, WindSpeedGustWindSpeedOfLocation, WindstormLossMitigation, Wiring, YearBuilt, HazardousSubstanceARate, CFLocationXmlId, LocationBuildingNumber)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
	DCCFBUILDINGSTAGEID, 
	LINEID, 
	CFLOCATIONID, 
	CFBUILDINGID, 
	SESSIONID, 
	ID, 
	o_Deleted AS DELETED, 
	AGREEDVALUE, 
	APPLICABLECOUNTSTORED, 
	o_Apply AS APPLY, 
	BG1SPECIFICOCCUPANCYGROUPLA, 
	BG2MASONRY, 
	BG2RISE, 
	BG2STEEL, 
	BG2SYMBOL, 
	BG2SYMBOLPREFIX, 
	BG2TERRITORYHI, 
	BLANKETCOINSURANCE, 
	BRANDSANDLABELS, 
	BUILDINGCODEEFFECTIVENESSGRADING, 
	BUILDINGGROUP, 
	BUILDINGTYPE, 
	BURGLARALARM, 
	CLASSLIMIT, 
	COMPOSITE, 
	CONDOMINIUM, 
	CONSTRUCTIONCODE, 
	CONTENTSHIGHLIMITSTORED, 
	o_Conversion AS CONVERSION, 
	CSP, 
	o_DCApartmentOption AS DCAPARTMENTOPTION, 
	o_DCOfficeOption AS DCOFFICEOPTION, 
	o_DebrisRemoval AS DEBRISREMOVAL, 
	DEDUCTIBLE, 
	DESCRIPTION, 
	DESIGNCODE, 
	DESIGNEXPOSURE, 
	DOORTYPE, 
	EARTHQUAKEBUILDINGCLASS, 
	o_Exposures AS EXPOSURES, 
	EXTERIORWALLS, 
	FUNCTIONALVALUATION, 
	o_GreenUpgrades AS GREENUPGRADES, 
	GREENUPGRADESPERIODOFRESTORATION, 
	o_HazardousConditions AS HAZARDOUSCONDITIONS, 
	o_HeatingCooking AS HEATINGCOOKING, 
	o_Housekeeping AS HOUSEKEEPING, 
	INTERNALPRESSUREDESIGN, 
	o_IsDwelling AS ISDWELLING, 
	MANUFACTURERSPERSONALPROPERTY, 
	MANUFACTURERSSTOCK, 
	o_MineSubLossAssessCondo AS MINESUBLOSSASSESSCONDO, 
	o_MultipleOccupancies AS MULTIPLEOCCUPANCIES, 
	MULTIPLEOCCUPANCIESCLASSCODE, 
	o_NewResidentialConstructionCredit AS NEWRESIDENTIALCONSTRUCTIONCREDIT, 
	NUMBEROFSTORIES, 
	OPENINGPROTECTION, 
	OTHERPROTECTIVEDEVICE, 
	o_PhysicalCondition AS PHYSICALCONDITION, 
	o_PhysicalConditionWA AS PHYSICALCONDITIONWA, 
	o_PierOrWharf AS PIERORWHARF, 
	PIERORWHARFCAUSEOFLOSS, 
	PREMIUMINSULATOR, 
	ROOFCOVERING, 
	ROOFCOVERINGSC, 
	ROOFDECK, 
	ROOFDECKATTACHMENT, 
	ROOFGEOMETRY, 
	ROOFSHAPE, 
	ROOFWALLCONNECTION, 
	ROOFWALLCONSTRUCTION, 
	SCAREA, 
	o_SecondaryWaterResistance AS SECONDARYWATERRESISTANCE, 
	o_Sprinkler AS SPRINKLER, 
	SPRINKLERLEAKAGEEXCLUDE, 
	SQUAREFT, 
	o_SubstandardCondition AS SUBSTANDARDCONDITION, 
	o_TenantRelocationExpenseEndorsementApplicable AS TENANTRELOCATIONEXPENSEENDORSEMENTAPPLICABLE, 
	TERRAIN, 
	o_UnderConstruction AS UNDERCONSTRUCTION, 
	UTILITYSERVICESDIRECTDAMAGE, 
	o_VacancyPermit AS VACANCYPERMIT, 
	o_VacancyPermitExcludeSprinklerLeakage AS VACANCYPERMITEXCLUDESPRINKLERLEAKAGE, 
	o_VacancyPermitExcludeVandalism AS VACANCYPERMITEXCLUDEVANDALISM, 
	o_VacantBuilding AS VACANTBUILDING, 
	o_VandalismExclude AS VANDALISMEXCLUDE, 
	WATCHMANPROTECTION, 
	o_WindBorneDebrisRegion AS WINDBORNEDEBRISREGION, 
	o_WindHailExcludeSelect AS WINDHAILEXCLUDESELECT, 
	WINDOWPROTECTION, 
	WINDSPEEDDESIGNSPEED, 
	WINDSPEEDGUSTWINDSPEEDOFDESIGN, 
	WINDSPEEDGUSTWINDSPEEDOFLOCATION, 
	o_WindstormLossMitigation AS WINDSTORMLOSSMITIGATION, 
	o_Wiring AS WIRING, 
	YEARBUILT, 
	HAZARDOUSSUBSTANCEARATE, 
	CFLOCATIONXMLID, 
	LOCATIONBUILDINGNUMBER
	FROM EXP_Metadata
),