WITH
SQ_WB_BP_Risk AS (
	WITH cte_WBBPRisk(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.BP_RiskId, 
	X.WB_BP_RiskId, 
	X.SessionId, 
	X.FunctionalValuationReason, 
	X.DescribeOther, 
	X.VacantBuilding, 
	X.BlanketBuildingIneligible, 
	X.BlanketPersonalPropertyGroupID, 
	X.Message1, 
	X.Message2, 
	X.Message3, 
	X.SumOfLimits, 
	X.LocationID, 
	X.BuildingID, 
	X.BuildingNumber, 
	X.ProtectionClassOverride, 
	X.IncludesEarthquakeBuilding, 
	X.IncludesEarthquakePersonalProperty, 
	X.BlanketType 
	FROM
	WB_BP_Risk X
	inner join
	cte_WBBPRisk Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXPTRANS AS (
	SELECT
	BP_RiskId,
	WB_BP_RiskId,
	SessionId,
	FunctionalValuationReason,
	DescribeOther,
	VacantBuilding,
	-- *INF*: DECODE(i_FineArtsCoverageForBreakage,'T','1','F','0',NULL)
	DECODE(
	    i_FineArtsCoverageForBreakage,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_FineArtsCoverageForBreakage,
	BlanketBuildingIneligible,
	BlanketPersonalPropertyGroupID,
	Message1,
	Message2,
	Message3,
	SumOfLimits,
	LocationID,
	BuildingID,
	BuildingNumber,
	ProtectionClassOverride,
	IncludesEarthquakeBuilding AS i_IncludesEarthquakeBuilding,
	-- *INF*: DECODE(i_IncludesEarthquakeBuilding,'T','1','F','0',NULL)
	DECODE(
	    i_IncludesEarthquakeBuilding,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_IncludesEarthquakeBuilding,
	IncludesEarthquakePersonalProperty AS i_IncludesEarthquakePersonalProperty,
	-- *INF*: DECODE(i_IncludesEarthquakePersonalProperty,'T','1','F','0',NULL)
	DECODE(
	    i_IncludesEarthquakePersonalProperty,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_IncludesEarthquakePersonalProperty,
	BlanketType,
	SYSDATE AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_WB_BP_Risk
),
WBBPRiskStage AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.WBBPRiskStage;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.WBBPRiskStage
	(ExtractDate, SourceSystemId, BP_RiskId, WB_BP_RiskId, SessionId, FunctionalValuationReason, DescribeOther, VacantBuilding, BlanketBuildingIneligible, BlanketPersonalPropertyGroupID, IncludesEarthquakeBuilding, IncludesEarthquakePersonalProperty, BlanketType, Message1, Message2, Message3, SumOfLimits, LocationID, BuildingID, BuildingNumber, ProtectionClassOverride)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	BP_RISKID, 
	WB_BP_RISKID, 
	SESSIONID, 
	FUNCTIONALVALUATIONREASON, 
	DESCRIBEOTHER, 
	VACANTBUILDING, 
	BLANKETBUILDINGINELIGIBLE, 
	BLANKETPERSONALPROPERTYGROUPID, 
	o_IncludesEarthquakeBuilding AS INCLUDESEARTHQUAKEBUILDING, 
	o_IncludesEarthquakePersonalProperty AS INCLUDESEARTHQUAKEPERSONALPROPERTY, 
	BLANKETTYPE, 
	MESSAGE1, 
	MESSAGE2, 
	MESSAGE3, 
	SUMOFLIMITS, 
	LOCATIONID, 
	BUILDINGID, 
	BUILDINGNUMBER, 
	PROTECTIONCLASSOVERRIDE
	FROM EXPTRANS
),