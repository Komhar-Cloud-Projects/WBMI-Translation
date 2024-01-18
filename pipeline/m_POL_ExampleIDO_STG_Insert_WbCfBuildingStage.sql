WITH
SQ_WB_CF_Building AS (
	WITH cte_WBCFBuilding(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.CF_BuildingId, 
	X.WB_CF_BuildingId, 
	X.SessionId, 
	X.BuildingNumber, 
	X.FirstTimeOnBuildingScreen, 
	X.SprinkleredBuildingStoredValue, 
	X.UtilityServicesARateStoredValue, 
	X.UtilityServicesCommunicationSupplyStoredValue, 
	X.UtilityServicesDirectDamageIndicatorStoredValue, 
	X.UtilityServicesDirectDamagePropertyDescriptionStoredValue, 
	X.UtilityServicesIndicatorStoredValue, 
	X.UtilityServicesLimitStoredValue, 
	X.GolfCourseCoverageStoredValue, 
	X.GolfCourseDeductibleStoredValue, 
	X.GolfCourseLimitStoredValue, 
	X.SpecificRatedBG1RateStoredValue, 
	X.SpecificRatedBG2RateStoredValue, 
	X.SpecificRatedEffectiveDateStoredValue, 
	X.UtilityServicesPowerSupplyStoredValue, 
	X.UtilityServicesProviderTypeStoredValue, 
	X.UtilityServicesWaterSupplyStoredValue, 
	X.SpecificRatedStoredValue, 
	X.SprinklerPercentageStoredValue, 
	X.SprinklerProtectionDeviceStoredValue, 
	X.SpecificRatedRCPStoredValue, 
	X.SpecificRatedRateStatusStoredValue, 
	X.SpecificRatedRiskIDStoredValue, 
	X.OtherSprinklerProtectionDeviceStoredValue 
	FROM
	WB_CF_Building X
	inner join
	cte_WBCFBuilding Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	CF_BuildingId,
	WB_CF_BuildingId,
	SessionId,
	BuildingNumber,
	FirstTimeOnBuildingScreen,
	-- *INF*: DECODE(FirstTimeOnBuildingScreen,'T',1,'F',0,NULL)
	DECODE(
	    FirstTimeOnBuildingScreen,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_FirstTimeOnBuildingScreen,
	SprinkleredBuildingStoredValue,
	UtilityServicesARateStoredValue,
	UtilityServicesCommunicationSupplyStoredValue,
	UtilityServicesDirectDamageIndicatorStoredValue,
	UtilityServicesDirectDamagePropertyDescriptionStoredValue,
	UtilityServicesIndicatorStoredValue,
	UtilityServicesLimitStoredValue,
	GolfCourseCoverageStoredValue,
	GolfCourseDeductibleStoredValue,
	GolfCourseLimitStoredValue,
	SpecificRatedBG1RateStoredValue,
	SpecificRatedBG2RateStoredValue,
	SpecificRatedEffectiveDateStoredValue,
	UtilityServicesPowerSupplyStoredValue,
	UtilityServicesProviderTypeStoredValue,
	UtilityServicesWaterSupplyStoredValue,
	SpecificRatedStoredValue,
	SprinklerPercentageStoredValue,
	SprinklerProtectionDeviceStoredValue,
	SpecificRatedRCPStoredValue,
	SpecificRatedRateStatusStoredValue,
	SpecificRatedRiskIDStoredValue,
	OtherSprinklerProtectionDeviceStoredValue,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_WB_CF_Building
),
WbCfBuildingStage AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.WbCfBuildingStage;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.WbCfBuildingStage
	(ExtractDate, SourceSystemId, CFBuildingId, WBCFBuildingId, SessionId, BuildingNumber, FirstTimeOnBuildingScreen, SprinkleredBuildingStoredValue, UtilityServicesARateStoredValue, UtilityServicesCommunicationSupplyStoredValue, UtilityServicesDirectDamageIndicatorStoredValue, UtilityServicesDirectDamagePropertyDescriptionStoredValue, UtilityServicesIndicatorStoredValue, UtilityServicesLimitStoredValue, GolfCourseCoverageStoredValue, GolfCourseDeductibleStoredValue, GolfCourseLimitStoredValue, SpecificRatedBG1RateStoredValue, SpecificRatedBG2RateStoredValue, SpecificRatedEffectiveDateStoredValue, UtilityServicesPowerSupplyStoredValue, UtilityServicesProviderTypeStoredValue, UtilityServicesWaterSupplyStoredValue, SpecificRatedStoredValue, SprinklerPercentageStoredValue, SprinklerProtectionDeviceStoredValue, SpecificRatedRCPStoredValue, SpecificRatedRateStatusStoredValue, SpecificRatedRiskIDStoredValue, OtherSprinklerProtectionDeviceStoredValue)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	CF_BuildingId AS CFBUILDINGID, 
	WB_CF_BuildingId AS WBCFBUILDINGID, 
	SESSIONID, 
	BUILDINGNUMBER, 
	o_FirstTimeOnBuildingScreen AS FIRSTTIMEONBUILDINGSCREEN, 
	SPRINKLEREDBUILDINGSTOREDVALUE, 
	UTILITYSERVICESARATESTOREDVALUE, 
	UTILITYSERVICESCOMMUNICATIONSUPPLYSTOREDVALUE, 
	UTILITYSERVICESDIRECTDAMAGEINDICATORSTOREDVALUE, 
	UTILITYSERVICESDIRECTDAMAGEPROPERTYDESCRIPTIONSTOREDVALUE, 
	UTILITYSERVICESINDICATORSTOREDVALUE, 
	UTILITYSERVICESLIMITSTOREDVALUE, 
	GOLFCOURSECOVERAGESTOREDVALUE, 
	GOLFCOURSEDEDUCTIBLESTOREDVALUE, 
	GOLFCOURSELIMITSTOREDVALUE, 
	SPECIFICRATEDBG1RATESTOREDVALUE, 
	SPECIFICRATEDBG2RATESTOREDVALUE, 
	SPECIFICRATEDEFFECTIVEDATESTOREDVALUE, 
	UTILITYSERVICESPOWERSUPPLYSTOREDVALUE, 
	UTILITYSERVICESPROVIDERTYPESTOREDVALUE, 
	UTILITYSERVICESWATERSUPPLYSTOREDVALUE, 
	SPECIFICRATEDSTOREDVALUE, 
	SPRINKLERPERCENTAGESTOREDVALUE, 
	SPRINKLERPROTECTIONDEVICESTOREDVALUE, 
	SPECIFICRATEDRCPSTOREDVALUE, 
	SPECIFICRATEDRATESTATUSSTOREDVALUE, 
	SPECIFICRATEDRISKIDSTOREDVALUE, 
	OTHERSPRINKLERPROTECTIONDEVICESTOREDVALUE
	FROM EXP_Metadata
),