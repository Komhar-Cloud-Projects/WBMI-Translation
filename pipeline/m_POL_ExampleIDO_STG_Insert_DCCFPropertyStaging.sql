WITH
SQ_DC_CF_Property AS (
	WITH cte_DCCFProperty(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.CF_RiskId, 
	X.CF_PropertyId, 
	X.SessionId, 
	X.Id, 
	X.BG1Coverages, 
	X.BG1PremRatingGroup, 
	X.BG1SpecificOccupancyGroupLA, 
	X.BG1SpecificRate, 
	X.BG1SpecificRateForNY, 
	X.BG1SpecificRateLCMSelect, 
	X.BG1SpecificRateSelect, 
	X.BG2HeavyMachinery, 
	X.BG2PremRatingGroup, 
	X.BG2SpecificRate, 
	X.BG2Symbol, 
	X.BG2SymbolApply, 
	X.BG2SymbolPrefix, 
	X.BG2TrailerAutoHomesMS, 
	X.BlanketHighestRateNote, 
	X.BuildingMaterialsSuppliesOfOthersPremiumBLDRK, 
	X.BurglarAlarm, 
	X.BurglarAlarmExtentOfProtection, 
	X.BurglarAlarmGrade, 
	X.CauseOfLoss, 
	X.ClassLimit, 
	X.CoinsurancePercentage, 
	X.CoinsurancePercentageGroupRated, 
	X.CoinsuranceRequirement, 
	X.CoinsuranceValueReporting, 
	X.CollapseBLDRK, 
	X.CommunicationSupply, 
	X.ComputerFailure, 
	X.ContentType, 
	X.CSP, 
	X.EarthquakeSelect, 
	X.EligibleForDeductibleInsurancePlan, 
	X.ExteriorWallsOfClayMS, 
	X.IncidentalApartment, 
	X.LimitedSprinklerExposure, 
	X.NonSprinkleredBuilding, 
	X.OccupancyType, 
	X.OtherCauseOfLossRiskType, 
	X.OtherPremRatingGroup, 
	X.OtherProtectiveDeviceFactor, 
	X.OtherProtectiveDevices, 
	X.PowerSupply, 
	X.ProviderType, 
	X.PublicPropertySelect, 
	X.RenovationsBLDRK, 
	X.ReportingFormBLDRK, 
	X.SpecializedUse, 
	X.SprinkleredBuilding, 
	X.SprinklerPercentage, 
	X.StateOwnedPropCredit, 
	X.SubContractors, 
	X.TenantsImprovementsBetterments, 
	X.TheftOfBuildingMaterialsBLDRK, 
	X.UnitNumber, 
	X.Valuation, 
	X.WatchmanProtection, 
	X.WaterSupply, 
	X.WindstormProtectiveDevices, 
	X.YardConstruction 
	FROM
	DC_CF_Property X
	inner join
	cte_DCCFProperty Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	CF_RiskId,
	CF_PropertyId,
	SessionId,
	Id,
	BG1Coverages,
	BG1PremRatingGroup,
	BG1SpecificOccupancyGroupLA,
	BG1SpecificRate,
	BG1SpecificRateForNY,
	BG1SpecificRateLCMSelect,
	BG1SpecificRateSelect,
	BG2HeavyMachinery,
	BG2PremRatingGroup,
	BG2SpecificRate,
	BG2Symbol,
	BG2SymbolApply,
	BG2SymbolPrefix,
	BG2TrailerAutoHomesMS,
	BlanketHighestRateNote,
	BuildingMaterialsSuppliesOfOthersPremiumBLDRK,
	BurglarAlarm,
	BurglarAlarmExtentOfProtection,
	BurglarAlarmGrade,
	CauseOfLoss,
	ClassLimit,
	CoinsurancePercentage,
	CoinsurancePercentageGroupRated,
	CoinsuranceRequirement,
	CoinsuranceValueReporting,
	CollapseBLDRK,
	CommunicationSupply,
	ComputerFailure,
	ContentType,
	CSP,
	EarthquakeSelect,
	EligibleForDeductibleInsurancePlan,
	ExteriorWallsOfClayMS,
	IncidentalApartment,
	LimitedSprinklerExposure,
	NonSprinkleredBuilding,
	OccupancyType,
	OtherCauseOfLossRiskType,
	OtherPremRatingGroup,
	OtherProtectiveDeviceFactor,
	OtherProtectiveDevices,
	PowerSupply,
	ProviderType,
	PublicPropertySelect,
	RenovationsBLDRK,
	ReportingFormBLDRK,
	SpecializedUse,
	SprinkleredBuilding,
	SprinklerPercentage,
	StateOwnedPropCredit,
	SubContractors,
	TenantsImprovementsBetterments,
	TheftOfBuildingMaterialsBLDRK,
	UnitNumber,
	Valuation,
	WatchmanProtection,
	WaterSupply,
	WindstormProtectiveDevices,
	YardConstruction,
	-- *INF*: DECODE(BG1SpecificRateForNY, 'T', 1, 'F', 0, NULL)
	DECODE(
	    BG1SpecificRateForNY,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_BG1SpecificRateForNY,
	-- *INF*: DECODE(BG1SpecificRateLCMSelect, 'T', 1, 'F', 0, NULL)
	DECODE(
	    BG1SpecificRateLCMSelect,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_BG1SpecificRateLCMSelect,
	-- *INF*: DECODE(BG1SpecificRateSelect, 'T', 1, 'F', 0, NULL)
	DECODE(
	    BG1SpecificRateSelect,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_BG1SpecificRateSelect,
	-- *INF*: DECODE(BG2SymbolApply, 'T', 1, 'F', 0, NULL)
	DECODE(
	    BG2SymbolApply,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_BG2SymbolApply,
	-- *INF*: DECODE(BG2TrailerAutoHomesMS, 'T', 1, 'F', 0, NULL)
	DECODE(
	    BG2TrailerAutoHomesMS,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_BG2TrailerAutoHomesMS,
	-- *INF*: DECODE(ComputerFailure, 'T', 1, 'F', 0, NULL)
	DECODE(
	    ComputerFailure,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_ComputerFailure,
	-- *INF*: DECODE(EarthquakeSelect, 'T', 1, 'F', 0, NULL)
	DECODE(
	    EarthquakeSelect,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_EarthquakeSelect,
	-- *INF*: DECODE(EligibleForDeductibleInsurancePlan, 'T', 1, 'F', 0, NULL)
	DECODE(
	    EligibleForDeductibleInsurancePlan,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_EligibleForDeductibleInsurancePlan,
	-- *INF*: DECODE(ExteriorWallsOfClayMS, 'T', 1, 'F', 0, NULL)
	DECODE(
	    ExteriorWallsOfClayMS,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_ExteriorWallsOfClayMS,
	-- *INF*: DECODE(IncidentalApartment, 'T', 1, 'F', 0, NULL)
	DECODE(
	    IncidentalApartment,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_IncidentalApartment,
	-- *INF*: DECODE(LimitedSprinklerExposure, 'T', 1, 'F', 0, NULL)
	DECODE(
	    LimitedSprinklerExposure,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_LimitedSprinklerExposure,
	-- *INF*: DECODE(NonSprinkleredBuilding, 'T', 1, 'F', 0, NULL)
	DECODE(
	    NonSprinkleredBuilding,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_NonSprinkleredBuilding,
	-- *INF*: DECODE(OtherProtectiveDevices, 'T', 1, 'F', 0, NULL)
	DECODE(
	    OtherProtectiveDevices,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_OtherProtectiveDevices,
	-- *INF*: DECODE(PublicPropertySelect, 'T', 1, 'F', 0, NULL)
	DECODE(
	    PublicPropertySelect,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_PublicPropertySelect,
	-- *INF*: DECODE(RenovationsBLDRK, 'T', 1, 'F', 0, NULL)
	DECODE(
	    RenovationsBLDRK,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_RenovationsBLDRK,
	-- *INF*: DECODE(ReportingFormBLDRK, 'T', 1, 'F', 0, NULL)
	DECODE(
	    ReportingFormBLDRK,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_ReportingFormBLDRK,
	-- *INF*: DECODE(SprinkleredBuilding, 'T', 1, 'F', 0, NULL)
	DECODE(
	    SprinkleredBuilding,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_SprinkleredBuilding,
	-- *INF*: DECODE(StateOwnedPropCredit, 'T', 1, 'F', 0, NULL)
	DECODE(
	    StateOwnedPropCredit,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_StateOwnedPropCredit,
	-- *INF*: DECODE(TenantsImprovementsBetterments, 'T', 1, 'F', 0, NULL)
	DECODE(
	    TenantsImprovementsBetterments,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_TenantsImprovementsBetterments,
	-- *INF*: DECODE(TheftOfBuildingMaterialsBLDRK, 'T', 1, 'F', 0, NULL)
	DECODE(
	    TheftOfBuildingMaterialsBLDRK,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_TheftOfBuildingMaterialsBLDRK,
	-- *INF*: DECODE(WaterSupply, 'T', 1, 'F', 0, NULL)
	DECODE(
	    WaterSupply,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_WaterSupply,
	-- *INF*: DECODE(WindstormProtectiveDevices, 'T', 1, 'F', 0, NULL)
	DECODE(
	    WindstormProtectiveDevices,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_WindstormProtectiveDevices,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_DC_CF_Property
),
DCCFPropertyStaging AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.DCCFPropertyStaging;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.DCCFPropertyStaging
	(CF_PropertyId, SessionId, Id, BG1Coverages, BG1PremRatingGroup, BG1SpecificOccupancyGroupLA, BG1SpecificRate, BG1SpecificRateForNY, BG1SpecificRateLCMSelect, BG1SpecificRateSelect, BG2HeavyMachinery, BG2PremRatingGroup, BG2SpecificRate, BG2Symbol, BG2SymbolApply, BG2SymbolPrefix, BG2TrailerAutoHomesMS, BlanketHighestRateNote, BuildingMaterialsSuppliesOfOthersPremiumBLDRK, BurglarAlarm, BurglarAlarmExtentOfProtection, BurglarAlarmGrade, CauseOfLoss, ClassLimit, CoinsurancePercentage, CoinsurancePercentageGroupRated, CoinsuranceRequirement, CoinsuranceValueReporting, CollapseBLDRK, CommunicationSupply, ComputerFailure, ContentType, CSP, EarthquakeSelect, EligibleForDeductibleInsurancePlan, ExteriorWallsOfClayMS, IncidentalApartment, LimitedSprinklerExposure, NonSprinkleredBuilding, OccupancyType, OtherCauseOfLossRiskType, OtherPremRatingGroup, OtherProtectiveDeviceFactor, OtherProtectiveDevices, PowerSupply, ProviderType, PublicPropertySelect, RenovationsBLDRK, ReportingFormBLDRK, SpecializedUse, SprinkleredBuilding, SprinklerPercentage, StateOwnedPropCredit, SubContractors, TenantsImprovementsBetterments, TheftOfBuildingMaterialsBLDRK, UnitNumber, Valuation, WatchmanProtection, WaterSupply, WindstormProtectiveDevices, YardConstruction, ExtractDate, SourceSystemId, CF_RiskId)
	SELECT 
	CF_PROPERTYID, 
	SESSIONID, 
	ID, 
	BG1COVERAGES, 
	BG1PREMRATINGGROUP, 
	BG1SPECIFICOCCUPANCYGROUPLA, 
	BG1SPECIFICRATE, 
	o_BG1SpecificRateForNY AS BG1SPECIFICRATEFORNY, 
	o_BG1SpecificRateLCMSelect AS BG1SPECIFICRATELCMSELECT, 
	o_BG1SpecificRateSelect AS BG1SPECIFICRATESELECT, 
	BG2HEAVYMACHINERY, 
	BG2PREMRATINGGROUP, 
	BG2SPECIFICRATE, 
	BG2SYMBOL, 
	o_BG2SymbolApply AS BG2SYMBOLAPPLY, 
	BG2SYMBOLPREFIX, 
	o_BG2TrailerAutoHomesMS AS BG2TRAILERAUTOHOMESMS, 
	BLANKETHIGHESTRATENOTE, 
	BUILDINGMATERIALSSUPPLIESOFOTHERSPREMIUMBLDRK, 
	BURGLARALARM, 
	BURGLARALARMEXTENTOFPROTECTION, 
	BURGLARALARMGRADE, 
	CAUSEOFLOSS, 
	CLASSLIMIT, 
	COINSURANCEPERCENTAGE, 
	COINSURANCEPERCENTAGEGROUPRATED, 
	COINSURANCEREQUIREMENT, 
	COINSURANCEVALUEREPORTING, 
	COLLAPSEBLDRK, 
	COMMUNICATIONSUPPLY, 
	o_ComputerFailure AS COMPUTERFAILURE, 
	CONTENTTYPE, 
	CSP, 
	o_EarthquakeSelect AS EARTHQUAKESELECT, 
	o_EligibleForDeductibleInsurancePlan AS ELIGIBLEFORDEDUCTIBLEINSURANCEPLAN, 
	o_ExteriorWallsOfClayMS AS EXTERIORWALLSOFCLAYMS, 
	o_IncidentalApartment AS INCIDENTALAPARTMENT, 
	o_LimitedSprinklerExposure AS LIMITEDSPRINKLEREXPOSURE, 
	o_NonSprinkleredBuilding AS NONSPRINKLEREDBUILDING, 
	OCCUPANCYTYPE, 
	OTHERCAUSEOFLOSSRISKTYPE, 
	OTHERPREMRATINGGROUP, 
	OTHERPROTECTIVEDEVICEFACTOR, 
	o_OtherProtectiveDevices AS OTHERPROTECTIVEDEVICES, 
	POWERSUPPLY, 
	PROVIDERTYPE, 
	o_PublicPropertySelect AS PUBLICPROPERTYSELECT, 
	o_RenovationsBLDRK AS RENOVATIONSBLDRK, 
	o_ReportingFormBLDRK AS REPORTINGFORMBLDRK, 
	SPECIALIZEDUSE, 
	o_SprinkleredBuilding AS SPRINKLEREDBUILDING, 
	SPRINKLERPERCENTAGE, 
	o_StateOwnedPropCredit AS STATEOWNEDPROPCREDIT, 
	SUBCONTRACTORS, 
	o_TenantsImprovementsBetterments AS TENANTSIMPROVEMENTSBETTERMENTS, 
	o_TheftOfBuildingMaterialsBLDRK AS THEFTOFBUILDINGMATERIALSBLDRK, 
	UNITNUMBER, 
	VALUATION, 
	WATCHMANPROTECTION, 
	o_WaterSupply AS WATERSUPPLY, 
	o_WindstormProtectiveDevices AS WINDSTORMPROTECTIVEDEVICES, 
	YARDCONSTRUCTION, 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	CF_RISKID
	FROM EXP_Metadata
),