WITH
SQ_DC_CF_BuildingRisk AS (
	WITH cte_DCCFBuildingRisk(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.CF_RiskId, 
	X.CF_BuildingRiskId, 
	X.SessionId, 
	X.Id, 
	X.MultipleResidential, 
	X.RiskBldgInputWholesaleOrStorage, 
	X.SpecialClassRating, 
	X.SubStandardConversion, 
	X.SubStandardExposure, 
	X.SubStandardHeatingCooking, 
	X.SubStandardPhysicalCondition, 
	X.SubStandardWiring, 
	X.Yard, 
	X.SpecialClassLevel1, 
	X.SpecialClassLevel2,
	X.RoofSurfacingCoverageLimitations
	FROM
	DC_CF_BuildingRisk X
	inner join
	cte_DCCFBuildingRisk Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	CF_RiskId,
	CF_BuildingRiskId,
	SessionId,
	Id,
	MultipleResidential AS i_MultipleResidential,
	-- *INF*: DECODE(i_MultipleResidential,'T',1,'F',0,NULL)
	DECODE(
	    i_MultipleResidential,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_MultipleResidential,
	RiskBldgInputWholesaleOrStorage AS i_RiskBldgInputWholesaleOrStorage,
	-- *INF*: DECODE(i_RiskBldgInputWholesaleOrStorage,'T',1,'F',0,NULL)
	DECODE(
	    i_RiskBldgInputWholesaleOrStorage,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_RiskBldgInputWholesaleOrStorage,
	SpecialClassRating AS i_SpecialClassRating,
	-- *INF*: DECODE(i_SpecialClassRating,'T',1,'F',0,NULL)
	DECODE(
	    i_SpecialClassRating,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_SpecialClassRating,
	SubStandardConversion AS i_SubStandardConversion,
	-- *INF*: DECODE(i_SubStandardConversion,'T',1,'F',0,NULL)
	DECODE(
	    i_SubStandardConversion,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_SubStandardConversion,
	SubStandardExposure AS i_SubStandardExposure,
	-- *INF*: DECODE(i_SubStandardExposure,'T',1,'F',0,NULL)
	DECODE(
	    i_SubStandardExposure,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_SubStandardExposure,
	SubStandardHeatingCooking AS i_SubStandardHeatingCooking,
	-- *INF*: DECODE(i_SubStandardHeatingCooking,'T',1,'F',0,NULL)
	DECODE(
	    i_SubStandardHeatingCooking,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_SubStandardHeatingCooking,
	SubStandardPhysicalCondition AS i_SubStandardPhysicalCondition,
	-- *INF*: DECODE(i_SubStandardPhysicalCondition,'T',1,'F',0,NULL)
	DECODE(
	    i_SubStandardPhysicalCondition,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_SubStandardPhysicalCondition,
	SubStandardWiring AS i_SubStandardWiring,
	-- *INF*: DECODE(i_SubStandardWiring,'T',1,'F',0,NULL)
	DECODE(
	    i_SubStandardWiring,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_SubStandardWiring,
	Yard AS i_Yard,
	-- *INF*: DECODE(i_Yard,'T',1,'F',0,NULL)
	DECODE(
	    i_Yard,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_Yard,
	SpecialClassLevel1,
	SpecialClassLevel2,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	RoofSurfacingCoverageLimitations,
	-- *INF*: SUBSTR(RoofSurfacingCoverageLimitations,0,100)
	SUBSTR(RoofSurfacingCoverageLimitations, 0, 100) AS o_RoofSurfacingCoverageLimitations
	FROM SQ_DC_CF_BuildingRisk
),
DCCFBuildingRiskStaging AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.DCCFBuildingRiskStaging;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.DCCFBuildingRiskStaging
	(CF_BuildingRiskId, SessionId, Id, MultipleResidential, RiskBldgInputWholesaleOrStorage, SpecialClassRating, SubStandardConversion, SubStandardExposure, SubStandardHeatingCooking, SubStandardPhysicalCondition, SubStandardWiring, Yard, SpecialClassLevel1, SpecialClassLevel2, ExtractDate, SourceSystemId, CF_RiskId, RoofSurfacingCoverageLimitations)
	SELECT 
	CF_BUILDINGRISKID, 
	SESSIONID, 
	ID, 
	o_MultipleResidential AS MULTIPLERESIDENTIAL, 
	o_RiskBldgInputWholesaleOrStorage AS RISKBLDGINPUTWHOLESALEORSTORAGE, 
	o_SpecialClassRating AS SPECIALCLASSRATING, 
	o_SubStandardConversion AS SUBSTANDARDCONVERSION, 
	o_SubStandardExposure AS SUBSTANDARDEXPOSURE, 
	o_SubStandardHeatingCooking AS SUBSTANDARDHEATINGCOOKING, 
	o_SubStandardPhysicalCondition AS SUBSTANDARDPHYSICALCONDITION, 
	o_SubStandardWiring AS SUBSTANDARDWIRING, 
	o_Yard AS YARD, 
	SPECIALCLASSLEVEL1, 
	SPECIALCLASSLEVEL2, 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	CF_RISKID, 
	o_RoofSurfacingCoverageLimitations AS ROOFSURFACINGCOVERAGELIMITATIONS
	FROM EXP_Metadata
),