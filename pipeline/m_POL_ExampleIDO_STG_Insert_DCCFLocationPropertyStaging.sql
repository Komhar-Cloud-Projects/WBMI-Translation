WITH
SQ_DC_CF_LocationProperty AS (
	WITH cte_DCCFLocationProperty(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.CF_LocationId, 
	X.CF_LocationPropertyId, 
	X.SessionId, 
	X.Id, 
	X.BG2TerritoryOverride, 
	X.BuildingCodeEffectivenessGradingApplicableToEqk, 
	X.BuildingCodeEffectivenessGradingApplicableToWindHail, 
	X.District, 
	X.EarthquakeTerritoryOrZone, 
	X.NonResidentialBuildingCodeEffectivenessGrading, 
	X.ProtectionClass, 
	X.ResidentialBuildingCodeEffectivenessGrading, 
	X.Territory, 
	X.TerrorismProgramYear, 
	X.TerrorismRejected, 
	X.TerrorismSelect, 
	X.TerrorismTier, 
	X.TerrorismExcludeDomestic, 
	X.TerrorismExcludeNuclearBiologicalChemicalDomestic, 
	X.TerrorismExcludeNuclearBiologicalChemicalCertified, 
	X.IncreasedPollutantCleanup, 
	X.TerrorismLimitationOfCoverage, 
	X.WindHailDeductible, 
	X.WindHailExclude, 
	X.HurricaneExclude, 
	X.BuildingCodeEffectivenessGradingSelect, 
	X.BuildingCodeEffectivenessIndividuallyGraded, 
	X.NCTerritorialZoneIsBeach, 
	X.TerrorismExcludeFireFollowing, 
	X.LocationOtherCredits_BuildersRisk, 
	X.LocationOtherCredits_Building, 
	X.LocationOtherCredits_BusinessIncome, 
	X.LocationOtherCredits_EQK, 
	X.LocationOtherCredits_PersProp, 
	X.TerrorismARate, 
	X.BG2TerritoryForCodeEffectiveness, 
	X.BG2TerritoryForCodeEffectivenessEqk, 
	X.BG2Territory, 
	X.IsResidentialClass, 
	X.PollutantCleanupPremium 
	FROM
	DC_CF_LocationProperty X
	inner join
	cte_DCCFLocationProperty Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	CF_LocationId AS i_CF_LocationId,
	CF_LocationPropertyId AS i_CF_LocationPropertyId,
	SessionId AS i_SessionId,
	Id AS i_Id,
	BG2TerritoryOverride AS i_BG2TerritoryOverride,
	BuildingCodeEffectivenessGradingApplicableToEqk AS i_BuildingCodeEffectivenessGradingApplicableToEqk,
	BuildingCodeEffectivenessGradingApplicableToWindHail AS i_BuildingCodeEffectivenessGradingApplicableToWindHail,
	District AS i_District,
	EarthquakeTerritoryOrZone AS i_EarthquakeTerritoryOrZone,
	NonResidentialBuildingCodeEffectivenessGrading AS i_NonResidentialBuildingCodeEffectivenessGrading,
	ProtectionClass AS i_ProtectionClass,
	ResidentialBuildingCodeEffectivenessGrading AS i_ResidentialBuildingCodeEffectivenessGrading,
	Territory AS i_Territory,
	TerrorismProgramYear AS i_TerrorismProgramYear,
	TerrorismRejected AS i_TerrorismRejected,
	TerrorismSelect AS i_TerrorismSelect,
	TerrorismTier AS i_TerrorismTier,
	TerrorismExcludeDomestic AS i_TerrorismExcludeDomestic,
	TerrorismExcludeNuclearBiologicalChemicalDomestic AS i_TerrorismExcludeNuclearBiologicalChemicalDomestic,
	TerrorismExcludeNuclearBiologicalChemicalCertified AS i_TerrorismExcludeNuclearBiologicalChemicalCertified,
	IncreasedPollutantCleanup AS i_IncreasedPollutantCleanup,
	TerrorismLimitationOfCoverage AS i_TerrorismLimitationOfCoverage,
	WindHailDeductible AS i_WindHailDeductible,
	WindHailExclude AS i_WindHailExclude,
	HurricaneExclude AS i_HurricaneExclude,
	BuildingCodeEffectivenessGradingSelect AS i_BuildingCodeEffectivenessGradingSelect,
	BuildingCodeEffectivenessIndividuallyGraded AS i_BuildingCodeEffectivenessIndividuallyGraded,
	NCTerritorialZoneIsBeach AS i_NCTerritorialZoneIsBeach,
	TerrorismExcludeFireFollowing AS i_TerrorismExcludeFireFollowing,
	LocationOtherCredits_BuildersRisk AS i_LocationOtherCredits_BuildersRisk,
	LocationOtherCredits_Building AS i_LocationOtherCredits_Building,
	LocationOtherCredits_BusinessIncome AS i_LocationOtherCredits_BusinessIncome,
	LocationOtherCredits_EQK AS i_LocationOtherCredits_EQK,
	LocationOtherCredits_PersProp AS i_LocationOtherCredits_PersProp,
	TerrorismARate AS i_TerrorismARate,
	BG2TerritoryForCodeEffectiveness AS i_BG2TerritoryForCodeEffectiveness,
	BG2TerritoryForCodeEffectivenessEqk AS i_BG2TerritoryForCodeEffectivenessEqk,
	BG2Territory AS i_BG2Territory,
	IsResidentialClass AS i_IsResidentialClass,
	PollutantCleanupPremium AS i_PollutantCleanupPremium,
	i_CF_LocationPropertyId AS o_CF_LocationPropertyId,
	i_SessionId AS o_SessionId,
	i_Id AS o_Id,
	i_BG2TerritoryOverride AS o_BG2TerritoryOverride,
	i_BuildingCodeEffectivenessGradingApplicableToEqk AS o_BuildingCodeEffectivenessGradingApplicableToEqk,
	i_BuildingCodeEffectivenessGradingApplicableToWindHail AS o_BuildingCodeEffectivenessGradingApplicableToWindHail,
	i_District AS o_District,
	i_EarthquakeTerritoryOrZone AS o_EarthquakeTerritoryOrZone,
	i_NonResidentialBuildingCodeEffectivenessGrading AS o_NonResidentialBuildingCodeEffectivenessGrading,
	i_ProtectionClass AS o_ProtectionClass,
	i_ResidentialBuildingCodeEffectivenessGrading AS o_ResidentialBuildingCodeEffectivenessGrading,
	i_Territory AS o_Territory,
	i_TerrorismProgramYear AS o_TerrorismProgramYear,
	-- *INF*: DECODE(i_TerrorismRejected,'T',1,'F',0,NULL)
	DECODE(
	    i_TerrorismRejected,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_TerrorismRejected,
	-- *INF*: DECODE(i_TerrorismSelect,'T',1,'F',0,NULL)
	DECODE(
	    i_TerrorismSelect,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_TerrorismSelect,
	i_TerrorismTier AS o_TerrorismTier,
	-- *INF*: DECODE(i_TerrorismExcludeDomestic,'T',1,'F',0,NULL)
	DECODE(
	    i_TerrorismExcludeDomestic,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_TerrorismExcludeDomestic,
	-- *INF*: DECODE(i_TerrorismExcludeNuclearBiologicalChemicalDomestic,'T',1,'F',0,NULL)
	DECODE(
	    i_TerrorismExcludeNuclearBiologicalChemicalDomestic,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_TerrorismExcludeNuclearBiologicalChemicalDomestic,
	-- *INF*: DECODE(i_TerrorismExcludeNuclearBiologicalChemicalCertified,'T',1,'F',0,NULL)
	DECODE(
	    i_TerrorismExcludeNuclearBiologicalChemicalCertified,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_TerrorismExcludeNuclearBiologicalChemicalCertified,
	-- *INF*: DECODE(i_IncreasedPollutantCleanup,'T',1,'F',0,NULL)
	DECODE(
	    i_IncreasedPollutantCleanup,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_IncreasedPollutantCleanup,
	i_TerrorismLimitationOfCoverage AS o_TerrorismLimitationOfCoverage,
	i_WindHailDeductible AS o_WindHailDeductible,
	-- *INF*: DECODE(i_WindHailExclude,'T',1,'F',0,NULL)
	DECODE(
	    i_WindHailExclude,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_WindHailExclude,
	-- *INF*: DECODE(i_HurricaneExclude,'T',1,'F',0,NULL)
	DECODE(
	    i_HurricaneExclude,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_HurricaneExclude,
	i_BuildingCodeEffectivenessGradingSelect AS o_BuildingCodeEffectivenessGradingSelect,
	i_BuildingCodeEffectivenessIndividuallyGraded AS o_BuildingCodeEffectivenessIndividuallyGraded,
	-- *INF*: DECODE(i_NCTerritorialZoneIsBeach,'T',1,'F',0,NULL)
	DECODE(
	    i_NCTerritorialZoneIsBeach,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_NCTerritorialZoneIsBeach,
	-- *INF*: DECODE(i_TerrorismExcludeFireFollowing,'T',1,'F',0,NULL)
	DECODE(
	    i_TerrorismExcludeFireFollowing,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_TerrorismExcludeFireFollowing,
	-- *INF*: DECODE(i_LocationOtherCredits_BuildersRisk,'T',1,'F',0,NULL)
	DECODE(
	    i_LocationOtherCredits_BuildersRisk,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_LocationOtherCredits_BuildersRisk,
	-- *INF*: DECODE(i_LocationOtherCredits_Building,'T',1,'F',0,NULL)
	DECODE(
	    i_LocationOtherCredits_Building,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_LocationOtherCredits_Building,
	-- *INF*: DECODE(i_LocationOtherCredits_BusinessIncome,'T',1,'F',0,NULL)
	DECODE(
	    i_LocationOtherCredits_BusinessIncome,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_LocationOtherCredits_BusinessIncome,
	-- *INF*: DECODE(i_LocationOtherCredits_EQK,'T',1,'F',0,NULL)
	DECODE(
	    i_LocationOtherCredits_EQK,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_LocationOtherCredits_EQK,
	-- *INF*: DECODE(i_LocationOtherCredits_PersProp,'T',1,'F',0,NULL)
	DECODE(
	    i_LocationOtherCredits_PersProp,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_LocationOtherCredits_PersProp,
	i_TerrorismARate AS o_TerrorismARate,
	i_BG2TerritoryForCodeEffectiveness AS o_BG2TerritoryForCodeEffectiveness,
	i_BG2TerritoryForCodeEffectivenessEqk AS o_BG2TerritoryForCodeEffectivenessEqk,
	i_BG2Territory AS o_BG2Territory,
	i_IsResidentialClass AS o_IsResidentialClass,
	i_PollutantCleanupPremium AS o_PollutantCleanupPremium,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	i_CF_LocationId AS o_CF_LocationId
	FROM SQ_DC_CF_LocationProperty
),
DCCFLocationPropertyStaging AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.DCCFLocationPropertyStaging;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.DCCFLocationPropertyStaging
	(ExtractDate, SourceSystemId, CF_LocationId, CF_LocationPropertyId, SessionId, Id, BG2TerritoryOverride, BuildingCodeEffectivenessGradingApplicableToEqk, BuildingCodeEffectivenessGradingApplicableToWindHail, District, EarthquakeTerritoryOrZone, NonResidentialBuildingCodeEffectivenessGrading, ProtectionClass, ResidentialBuildingCodeEffectivenessGrading, Territory, TerrorismProgramYear, TerrorismRejected, TerrorismSelect, TerrorismTier, TerrorismExcludeNuclearBiologicalChemicalDomestic, TerrorismExcludeNuclearBiologicalChemicalCertified, IncreasedPollutantCleanup, TerrorismLimitationOfCoverage, WindHailDeductible, WindHailExclude, HurricaneExclude, BuildingCodeEffectivenessGradingSelect, BuildingCodeEffectivenessIndividuallyGraded, NCTerritorialZoneIsBeach, LocationOtherCredits_BuildersRisk, LocationOtherCredits_Building, LocationOtherCredits_BusinessIncome, LocationOtherCredits_EQK, LocationOtherCredits_PersProp, TerrorismARate, BG2TerritoryForCodeEffectiveness, BG2TerritoryForCodeEffectivenessEqk, BG2Territory, IsResidentialClass, PollutantCleanupPremium, TerrorismExcludeDomestic, TerrorismExcludeFireFollowing)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	o_CF_LocationId AS CF_LOCATIONID, 
	o_CF_LocationPropertyId AS CF_LOCATIONPROPERTYID, 
	o_SessionId AS SESSIONID, 
	o_Id AS ID, 
	o_BG2TerritoryOverride AS BG2TERRITORYOVERRIDE, 
	o_BuildingCodeEffectivenessGradingApplicableToEqk AS BUILDINGCODEEFFECTIVENESSGRADINGAPPLICABLETOEQK, 
	o_BuildingCodeEffectivenessGradingApplicableToWindHail AS BUILDINGCODEEFFECTIVENESSGRADINGAPPLICABLETOWINDHAIL, 
	o_District AS DISTRICT, 
	o_EarthquakeTerritoryOrZone AS EARTHQUAKETERRITORYORZONE, 
	o_NonResidentialBuildingCodeEffectivenessGrading AS NONRESIDENTIALBUILDINGCODEEFFECTIVENESSGRADING, 
	o_ProtectionClass AS PROTECTIONCLASS, 
	o_ResidentialBuildingCodeEffectivenessGrading AS RESIDENTIALBUILDINGCODEEFFECTIVENESSGRADING, 
	o_Territory AS TERRITORY, 
	o_TerrorismProgramYear AS TERRORISMPROGRAMYEAR, 
	o_TerrorismRejected AS TERRORISMREJECTED, 
	o_TerrorismSelect AS TERRORISMSELECT, 
	o_TerrorismTier AS TERRORISMTIER, 
	o_TerrorismExcludeNuclearBiologicalChemicalDomestic AS TERRORISMEXCLUDENUCLEARBIOLOGICALCHEMICALDOMESTIC, 
	o_TerrorismExcludeNuclearBiologicalChemicalCertified AS TERRORISMEXCLUDENUCLEARBIOLOGICALCHEMICALCERTIFIED, 
	o_IncreasedPollutantCleanup AS INCREASEDPOLLUTANTCLEANUP, 
	o_TerrorismLimitationOfCoverage AS TERRORISMLIMITATIONOFCOVERAGE, 
	o_WindHailDeductible AS WINDHAILDEDUCTIBLE, 
	o_WindHailExclude AS WINDHAILEXCLUDE, 
	o_HurricaneExclude AS HURRICANEEXCLUDE, 
	o_BuildingCodeEffectivenessGradingSelect AS BUILDINGCODEEFFECTIVENESSGRADINGSELECT, 
	o_BuildingCodeEffectivenessIndividuallyGraded AS BUILDINGCODEEFFECTIVENESSINDIVIDUALLYGRADED, 
	o_NCTerritorialZoneIsBeach AS NCTERRITORIALZONEISBEACH, 
	o_LocationOtherCredits_BuildersRisk AS LOCATIONOTHERCREDITS_BUILDERSRISK, 
	o_LocationOtherCredits_Building AS LOCATIONOTHERCREDITS_BUILDING, 
	o_LocationOtherCredits_BusinessIncome AS LOCATIONOTHERCREDITS_BUSINESSINCOME, 
	o_LocationOtherCredits_EQK AS LOCATIONOTHERCREDITS_EQK, 
	o_LocationOtherCredits_PersProp AS LOCATIONOTHERCREDITS_PERSPROP, 
	o_TerrorismARate AS TERRORISMARATE, 
	o_BG2TerritoryForCodeEffectiveness AS BG2TERRITORYFORCODEEFFECTIVENESS, 
	o_BG2TerritoryForCodeEffectivenessEqk AS BG2TERRITORYFORCODEEFFECTIVENESSEQK, 
	o_BG2Territory AS BG2TERRITORY, 
	o_IsResidentialClass AS ISRESIDENTIALCLASS, 
	o_PollutantCleanupPremium AS POLLUTANTCLEANUPPREMIUM, 
	o_TerrorismExcludeDomestic AS TERRORISMEXCLUDEDOMESTIC, 
	o_TerrorismExcludeFireFollowing AS TERRORISMEXCLUDEFIREFOLLOWING
	FROM EXP_Metadata
),