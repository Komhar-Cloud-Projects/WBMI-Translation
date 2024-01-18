WITH
SQ_DCCFBuildingRiskStaging AS (
	SELECT
		CF_BuildingRiskId,
		SessionId,
		Id,
		MultipleResidential,
		RiskBldgInputWholesaleOrStorage,
		SpecialClassRating,
		SubStandardConversion,
		SubStandardExposure,
		SubStandardHeatingCooking,
		SubStandardPhysicalCondition,
		SubStandardWiring,
		Yard,
		SpecialClassLevel1,
		SpecialClassLevel2,
		ExtractDate,
		SourceSystemId,
		CF_RiskId,
		RoofSurfacingCoverageLimitations
	FROM DCCFBuildingRiskStaging
),
EXP_Metadata AS (
	SELECT
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
	ExtractDate,
	SourceSystemId,
	CF_RiskId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	RoofSurfacingCoverageLimitations
	FROM SQ_DCCFBuildingRiskStaging
),
archDCCFBuildingRiskStaging AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.archDCCFBuildingRiskStaging
	(CF_RiskId, CF_BuildingRiskId, SessionId, Id, MultipleResidential, RiskBldgInputWholesaleOrStorage, SpecialClassRating, SubStandardConversion, SubStandardExposure, SubStandardHeatingCooking, SubStandardPhysicalCondition, SubStandardWiring, Yard, SpecialClassLevel1, SpecialClassLevel2, ExtractDate, SourceSystemId, AuditId, RoofSurfacingCoverageLimitations)
	SELECT 
	CF_RISKID, 
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
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
	ROOFSURFACINGCOVERAGELIMITATIONS
	FROM EXP_Metadata
),