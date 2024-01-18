WITH
SQ_DCCFPropertyStaging AS (
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
		ExtractDate,
		SourceSystemId
	FROM DCCFPropertyStaging
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
	BG1SpecificRateForNY AS i_BG1SpecificRateForNY,
	-- *INF*: DECODE(i_BG1SpecificRateForNY,'T',1,'F',0,NULL)
	DECODE(
	    i_BG1SpecificRateForNY,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_BG1SpecificRateForNY,
	BG1SpecificRateLCMSelect AS i_BG1SpecificRateLCMSelect,
	-- *INF*: DECODE(i_BG1SpecificRateLCMSelect,'T',1,'F',0,NULL)
	DECODE(
	    i_BG1SpecificRateLCMSelect,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_BG1SpecificRateLCMSelect,
	BG1SpecificRateSelect AS i_BG1SpecificRateSelect,
	-- *INF*: DECODE(i_BG1SpecificRateSelect,'T',1,'F',0,NULL)
	DECODE(
	    i_BG1SpecificRateSelect,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_BG1SpecificRateSelect,
	BG2HeavyMachinery,
	BG2PremRatingGroup,
	BG2SpecificRate,
	BG2Symbol,
	BG2SymbolApply AS i_BG2SymbolApply,
	-- *INF*: DECODE(i_BG2SymbolApply,'T',1,'F',0,NULL)
	DECODE(
	    i_BG2SymbolApply,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_BG2SymbolApply,
	BG2SymbolPrefix,
	BG2TrailerAutoHomesMS AS i_BG2TrailerAutoHomesMS,
	-- *INF*: DECODE(i_BG2TrailerAutoHomesMS,'T',1,'F',0,NULL)
	DECODE(
	    i_BG2TrailerAutoHomesMS,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_BG2TrailerAutoHomesMS,
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
	ComputerFailure AS i_ComputerFailure,
	-- *INF*: DECODE(i_ComputerFailure,'T',1,'F',0,NULL)
	DECODE(
	    i_ComputerFailure,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_ComputerFailure,
	ContentType,
	CSP,
	EarthquakeSelect AS i_EarthquakeSelect,
	-- *INF*: DECODE(i_EarthquakeSelect,'T',1,'F',0,NULL)
	DECODE(
	    i_EarthquakeSelect,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_EarthquakeSelect,
	EligibleForDeductibleInsurancePlan AS i_EligibleForDeductibleInsurancePlan,
	-- *INF*: DECODE(i_EligibleForDeductibleInsurancePlan,'T',1,'F',0,NULL)
	DECODE(
	    i_EligibleForDeductibleInsurancePlan,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_EligibleForDeductibleInsurancePlan,
	ExteriorWallsOfClayMS AS i_ExteriorWallsOfClayMS,
	-- *INF*: DECODE(i_ExteriorWallsOfClayMS,'T',1,'F',0,NULL)
	DECODE(
	    i_ExteriorWallsOfClayMS,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_ExteriorWallsOfClayMS,
	IncidentalApartment AS i_IncidentalApartment,
	-- *INF*: DECODE(i_IncidentalApartment,'T',1,'F',0,NULL)
	DECODE(
	    i_IncidentalApartment,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_IncidentalApartment,
	LimitedSprinklerExposure AS i_LimitedSprinklerExposure,
	-- *INF*: DECODE(i_LimitedSprinklerExposure,'T',1,'F',0,NULL)
	DECODE(
	    i_LimitedSprinklerExposure,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_LimitedSprinklerExposure,
	NonSprinkleredBuilding AS i_NonSprinkleredBuilding,
	-- *INF*: DECODE(i_NonSprinkleredBuilding,'T',1,'F',0,NULL)
	DECODE(
	    i_NonSprinkleredBuilding,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_NonSprinkleredBuilding,
	OccupancyType,
	OtherCauseOfLossRiskType,
	OtherPremRatingGroup,
	OtherProtectiveDeviceFactor,
	OtherProtectiveDevices AS i_OtherProtectiveDevices,
	-- *INF*: DECODE(i_OtherProtectiveDevices,'T',1,'F',0,NULL)
	DECODE(
	    i_OtherProtectiveDevices,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_OtherProtectiveDevices,
	PowerSupply,
	ProviderType,
	PublicPropertySelect AS i_PublicPropertySelect,
	-- *INF*: DECODE(i_PublicPropertySelect,'T',1,'F',0,NULL)
	DECODE(
	    i_PublicPropertySelect,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_PublicPropertySelect,
	RenovationsBLDRK AS i_RenovationsBLDRK,
	-- *INF*: DECODE(i_RenovationsBLDRK,'T',1,'F',0,NULL)
	DECODE(
	    i_RenovationsBLDRK,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_RenovationsBLDRK,
	ReportingFormBLDRK AS i_ReportingFormBLDRK,
	-- *INF*: DECODE(i_ReportingFormBLDRK,'T',1,'F',0,NULL)
	DECODE(
	    i_ReportingFormBLDRK,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_ReportingFormBLDRK,
	SpecializedUse,
	SprinkleredBuilding AS i_SprinkleredBuilding,
	-- *INF*: DECODE(i_SprinkleredBuilding,'T',1,'F',0,NULL)
	DECODE(
	    i_SprinkleredBuilding,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_SprinkleredBuilding,
	SprinklerPercentage,
	StateOwnedPropCredit AS i_StateOwnedPropCredit,
	-- *INF*: DECODE(i_StateOwnedPropCredit,'T',1,'F',0,NULL)
	DECODE(
	    i_StateOwnedPropCredit,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_StateOwnedPropCredit,
	SubContractors,
	TenantsImprovementsBetterments AS i_TenantsImprovementsBetterments,
	-- *INF*: DECODE(i_TenantsImprovementsBetterments,'T',1,'F',0,NULL)
	DECODE(
	    i_TenantsImprovementsBetterments,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_TenantsImprovementsBetterments,
	TheftOfBuildingMaterialsBLDRK AS i_TheftOfBuildingMaterialsBLDRK,
	-- *INF*: DECODE(i_TheftOfBuildingMaterialsBLDRK,'T',1,'F',0,NULL)
	DECODE(
	    i_TheftOfBuildingMaterialsBLDRK,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_TheftOfBuildingMaterialsBLDRK,
	UnitNumber,
	Valuation,
	WatchmanProtection,
	WaterSupply AS i_WaterSupply,
	-- *INF*: DECODE(i_WaterSupply,'T',1,'F',0,NULL)
	DECODE(
	    i_WaterSupply,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_WaterSupply,
	WindstormProtectiveDevices AS i_WindstormProtectiveDevices,
	-- *INF*: DECODE(i_WindstormProtectiveDevices,'T',1,'F',0,NULL)
	DECODE(
	    i_WindstormProtectiveDevices,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_WindstormProtectiveDevices,
	YardConstruction,
	ExtractDate,
	SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM SQ_DCCFPropertyStaging
),
archDCCFPropertyStaging AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.archDCCFPropertyStaging
	(CF_RiskId, CF_PropertyId, SessionId, Id, BG1Coverages, BG1PremRatingGroup, BG1SpecificOccupancyGroupLA, BG1SpecificRate, BG1SpecificRateForNY, BG1SpecificRateLCMSelect, BG1SpecificRateSelect, BG2HeavyMachinery, BG2PremRatingGroup, BG2SpecificRate, BG2Symbol, BG2SymbolApply, BG2SymbolPrefix, BG2TrailerAutoHomesMS, BlanketHighestRateNote, BuildingMaterialsSuppliesOfOthersPremiumBLDRK, BurglarAlarm, BurglarAlarmExtentOfProtection, BurglarAlarmGrade, CauseOfLoss, ClassLimit, CoinsurancePercentage, CoinsurancePercentageGroupRated, CoinsuranceRequirement, CoinsuranceValueReporting, CollapseBLDRK, CommunicationSupply, ComputerFailure, ContentType, CSP, EarthquakeSelect, EligibleForDeductibleInsurancePlan, ExteriorWallsOfClayMS, IncidentalApartment, LimitedSprinklerExposure, NonSprinkleredBuilding, OccupancyType, OtherCauseOfLossRiskType, OtherPremRatingGroup, OtherProtectiveDeviceFactor, OtherProtectiveDevices, PowerSupply, ProviderType, PublicPropertySelect, RenovationsBLDRK, ReportingFormBLDRK, SpecializedUse, SprinkleredBuilding, SprinklerPercentage, StateOwnedPropCredit, SubContractors, TenantsImprovementsBetterments, TheftOfBuildingMaterialsBLDRK, UnitNumber, Valuation, WatchmanProtection, WaterSupply, WindstormProtectiveDevices, YardConstruction, ExtractDate, SourceSystemId, AuditId)
	SELECT 
	CF_RISKID, 
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
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID
	FROM EXP_Metadata
),