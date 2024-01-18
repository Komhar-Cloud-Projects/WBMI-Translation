WITH
SQ_DC_CF_Building AS (
	WITH cte_DCCFBuilding(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.LineId,  
	X.CF_LocationId,  
	X.CF_BuildingId,  
	X.SessionId,  
	X.Id,  
	X.Deleted,  
	X.AgreedValue,  
	X.ApplicableCountStored,  
	X.Apply,  
	X.BG1SpecificOccupancyGroupLA,  
	X.BG2Masonry,  
	X.BG2Rise,  
	X.BG2Steel,  
	X.BG2Symbol,  
	X.BG2SymbolPrefix,  
	X.BG2TerritoryHI,  
	X.BlanketCoinsurance,  
	X.BrandsAndLabels,  
	X.BuildingCodeEffectivenessGrading,  
	X.BuildingGroup,  
	X.BuildingType,  
	X.BurglarAlarm,  
	X.ClassLimit,  
	X.Composite,  
	X.Condominium,  
	X.ConstructionCode,  
	X.ContentsHighLimitStored,  
	X.Conversion,  
	X.CSP,  
	X.DCApartmentOption,  
	X.DCOfficeOption,  
	X.DebrisRemoval,  
	X.Deductible,  
	X.Description,  
	X.DesignCode,  
	X.DesignExposure,  
	X.DoorType,  
	X.EarthquakeBuildingClass,  
	X.Exposures,  
	X.ExteriorWalls,  
	X.FunctionalValuation,  
	X.GreenUpgrades,  
	X.GreenUpgradesPeriodOfRestoration,  
	X.HazardousConditions,  
	X.HeatingCooking,  
	X.Housekeeping,  
	X.InternalPressureDesign,  
	X.IsDwelling,  
	X.ManufacturersPersonalProperty,  
	X.ManufacturersStock,  
	X.MineSubLossAssessCondo,  
	X.MultipleOccupancies,  
	X.MultipleOccupanciesClassCode,  
	X.NewResidentialConstructionCredit,  
	X.NumberOfStories,  
	X.OpeningProtection,  
	X.OtherProtectiveDevice,  
	X.PhysicalCondition,  
	X.PhysicalConditionWA,  
	X.PierOrWharf,  
	X.PierOrWharfCauseOfLoss,  
	X.PremiumInsulator,  
	X.RoofCovering,  
	X.RoofCoveringSC,  
	X.RoofDeck,  
	X.RoofDeckAttachment,  
	X.RoofGeometry,  
	X.RoofShape,  
	X.RoofWallConnection,  
	X.RoofWallConstruction,  
	X.SCArea,  
	X.SecondaryWaterResistance,  
	X.Sprinkler,  
	X.SprinklerLeakageExclude,  
	X.SquareFt,  
	X.SubstandardCondition,  
	X.TenantRelocationExpenseEndorsementApplicable,  
	X.Terrain,  
	X.UnderConstruction,  
	X.UtilityServicesDirectDamage,  
	X.VacancyPermit,  
	X.VacancyPermitExcludeSprinklerLeakage,  
	X.VacancyPermitExcludeVandalism,  
	X.VacantBuilding,  
	X.VandalismExclude,  
	X.WatchmanProtection,  
	X.WindBorneDebrisRegion,  
	X.WindHailExcludeSelect,  
	X.WindowProtection,  
	X.WindSpeedDesignSpeed,  
	X.WindSpeedGustWindSpeedOfDesign,  
	X.WindSpeedGustWindSpeedOfLocation,  
	X.WindstormLossMitigation,  
	X.Wiring,  
	X.YearBuilt,  
	X.HazardousSubstanceARate,
	X.LocationBuildingNumber,  
	X.CF_LocationXmlId  
	FROM DBO.DC_CF_Building X 
	inner join cte_DCCFBuilding Y
	on X.SessionId = Y.SessionId
),
EXP_Metadata AS (
	SELECT
	LineId,
	CF_LocationId,
	CF_BuildingId,
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
	CF_LocationXmlId,
	-- *INF*: DECODE(Apply, 'T', 1, 'F', 0, NULL)
	DECODE(
	    Apply,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_Apply,
	-- *INF*: DECODE(Conversion, 'T', 1, 'F', 0, NULL)
	DECODE(
	    Conversion,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_Conversion,
	-- *INF*: DECODE(DCApartmentOption, 'T', 1, 'F', 0, NULL)
	-- 
	DECODE(
	    DCApartmentOption,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_DCApartmentOption,
	-- *INF*: DECODE(DCOfficeOption, 'T', 1, 'F', 0, NULL)
	DECODE(
	    DCOfficeOption,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_DCOfficeOption,
	-- *INF*: DECODE(DebrisRemoval, 'T', 1, 'F', 0, NULL)
	DECODE(
	    DebrisRemoval,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_DebrisRemoval,
	-- *INF*: DECODE(Deleted, 'T', 1, 'F', 0, NULL)
	DECODE(
	    Deleted,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_Deleted,
	-- *INF*: DECODE(Exposures, 'T', 1, 'F', 0, NULL)
	DECODE(
	    Exposures,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_Exposures,
	-- *INF*: DECODE(GreenUpgrades, 'T', 1, 'F', 0, NULL)
	DECODE(
	    GreenUpgrades,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_GreenUpgrades,
	-- *INF*: DECODE(HazardousConditions, 'T', 1, 'F', 0, NULL)
	DECODE(
	    HazardousConditions,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_HazardousConditions,
	-- *INF*: DECODE(HeatingCooking, 'T', 1, 'F', 0, NULL)
	DECODE(
	    HeatingCooking,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_HeatingCooking,
	-- *INF*: DECODE(Housekeeping, 'T', 1, 'F', 0, NULL)
	-- 
	DECODE(
	    Housekeeping,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_Housekeeping,
	-- *INF*: DECODE(IsDwelling, 'T', 1, 'F', 0, NULL)
	DECODE(
	    IsDwelling,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_IsDwelling,
	-- *INF*: DECODE(MineSubLossAssessCondo, 'T', 1, 'F', 0, NULL)
	DECODE(
	    MineSubLossAssessCondo,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_MineSubLossAssessCondo,
	-- *INF*: DECODE(MultipleOccupancies, 'T', 1, 'F', 0, NULL)
	DECODE(
	    MultipleOccupancies,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_MultipleOccupancies,
	-- *INF*: DECODE(NewResidentialConstructionCredit, 'T', 1, 'F', 0, NULL)
	DECODE(
	    NewResidentialConstructionCredit,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_NewResidentialConstructionCredit,
	-- *INF*: DECODE(PhysicalCondition, 'T', 1, 'F', 0, NULL)
	DECODE(
	    PhysicalCondition,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_PhysicalCondition,
	-- *INF*: DECODE(PhysicalConditionWA, 'T', 1, 'F', 0, NULL)
	DECODE(
	    PhysicalConditionWA,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_PhysicalConditionWA,
	-- *INF*: DECODE(PierOrWharf, 'T', 1, 'F', 0, NULL)
	DECODE(
	    PierOrWharf,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_PierOrWharf,
	-- *INF*: DECODE(SecondaryWaterResistance, 'T', 1, 'F', 0, NULL)
	DECODE(
	    SecondaryWaterResistance,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_SecondaryWaterResistance,
	-- *INF*: DECODE(Sprinkler, 'T', 1, 'F', 0, NULL)
	DECODE(
	    Sprinkler,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_Sprinkler,
	-- *INF*: DECODE(SubstandardCondition, 'T', 1, 'F', 0, NULL)
	-- 
	DECODE(
	    SubstandardCondition,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_SubstandardCondition,
	-- *INF*: DECODE(TenantRelocationExpenseEndorsementApplicable, 'T', 1, 'F', 0, NULL)
	DECODE(
	    TenantRelocationExpenseEndorsementApplicable,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_TenantRelocationExpenseEndorsementApplicable,
	-- *INF*: DECODE(UnderConstruction, 'T', 1, 'F', 0, NULL)
	DECODE(
	    UnderConstruction,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_UnderConstruction,
	-- *INF*: DECODE(VacancyPermit, 'T', 1, 'F', 0, NULL)
	DECODE(
	    VacancyPermit,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_VacancyPermit,
	-- *INF*: DECODE(VacancyPermitExcludeSprinklerLeakage, 'T', 1, 'F', 0, NULL)
	DECODE(
	    VacancyPermitExcludeSprinklerLeakage,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_VacancyPermitExcludeSprinklerLeakage,
	-- *INF*: DECODE(VacancyPermitExcludeVandalism, 'T', 1, 'F', 0, NULL)
	DECODE(
	    VacancyPermitExcludeVandalism,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_VacancyPermitExcludeVandalism,
	-- *INF*: DECODE(VacantBuilding, 'T', 1, 'F', 0, NULL)
	DECODE(
	    VacantBuilding,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_VacantBuilding,
	-- *INF*: DECODE(VandalismExclude, 'T', 1, 'F', 0, NULL)
	DECODE(
	    VandalismExclude,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_VandalismExclude,
	-- *INF*: DECODE(WindBorneDebrisRegion, 'T', 1, 'F', 0, NULL)
	DECODE(
	    WindBorneDebrisRegion,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_WindBorneDebrisRegion,
	-- *INF*: DECODE(WindHailExcludeSelect, 'T', 1, 'F', 0, NULL)
	DECODE(
	    WindHailExcludeSelect,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_WindHailExcludeSelect,
	-- *INF*: DECODE(WindstormLossMitigation, 'T', 1, 'F', 0, NULL)
	DECODE(
	    WindstormLossMitigation,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_WindstormLossMitigation,
	-- *INF*: DECODE(Wiring, 'T', 1, 'F', 0, NULL)
	DECODE(
	    Wiring,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_Wiring,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	LocationBuildingNumber
	FROM SQ_DC_CF_Building
),
DCCFBuildingStage AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.DCCFBuildingStage;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.DCCFBuildingStage
	(ExtractDate, SourceSystemId, LineId, CFLocationId, CFBuildingId, SessionId, Id, Deleted, AgreedValue, ApplicableCountStored, Apply, BG1SpecificOccupancyGroupLA, BG2Masonry, BG2Rise, BG2Steel, BG2Symbol, BG2SymbolPrefix, BG2TerritoryHI, BlanketCoinsurance, BrandsAndLabels, BuildingCodeEffectivenessGrading, BuildingGroup, BuildingType, BurglarAlarm, ClassLimit, Composite, Condominium, ConstructionCode, ContentsHighLimitStored, Conversion, CSP, DCApartmentOption, DCOfficeOption, DebrisRemoval, Deductible, Description, DesignCode, DesignExposure, DoorType, EarthquakeBuildingClass, Exposures, ExteriorWalls, FunctionalValuation, GreenUpgrades, GreenUpgradesPeriodOfRestoration, HazardousConditions, HeatingCooking, Housekeeping, InternalPressureDesign, IsDwelling, ManufacturersPersonalProperty, ManufacturersStock, MineSubLossAssessCondo, MultipleOccupancies, MultipleOccupanciesClassCode, NewResidentialConstructionCredit, NumberOfStories, OpeningProtection, OtherProtectiveDevice, PhysicalCondition, PhysicalConditionWA, PierOrWharf, PierOrWharfCauseOfLoss, PremiumInsulator, RoofCovering, RoofCoveringSC, RoofDeck, RoofDeckAttachment, RoofGeometry, RoofShape, RoofWallConnection, RoofWallConstruction, SCArea, SecondaryWaterResistance, Sprinkler, SprinklerLeakageExclude, SquareFt, SubstandardCondition, TenantRelocationExpenseEndorsementApplicable, Terrain, UnderConstruction, UtilityServicesDirectDamage, VacancyPermit, VacancyPermitExcludeSprinklerLeakage, VacancyPermitExcludeVandalism, VacantBuilding, VandalismExclude, WatchmanProtection, WindBorneDebrisRegion, WindHailExcludeSelect, WindowProtection, WindSpeedDesignSpeed, WindSpeedGustWindSpeedOfDesign, WindSpeedGustWindSpeedOfLocation, WindstormLossMitigation, Wiring, YearBuilt, HazardousSubstanceARate, CFLocationXmlId, LocationBuildingNumber)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	LINEID, 
	CF_LocationId AS CFLOCATIONID, 
	CF_BuildingId AS CFBUILDINGID, 
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
	CF_LocationXmlId AS CFLOCATIONXMLID, 
	LOCATIONBUILDINGNUMBER
	FROM EXP_Metadata
),