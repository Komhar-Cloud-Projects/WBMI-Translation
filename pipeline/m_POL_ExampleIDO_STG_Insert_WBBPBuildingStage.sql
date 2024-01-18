WITH
SQ_WB_BP_Building AS (
	WITH cte_WBBPBuilding(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.BP_BuildingId, 
	X.WB_BP_BuildingId, 
	X.SessionId, 
	X.AlarmLocal, 
	X.AlarmCentralStation, 
	X.SafeClassBOrBetter, 
	X.SafeOther, 
	X.LocksDeadbolt, 
	X.LocksOther, 
	X.ClosedCircuitTV, 
	X.NoCrimeControls, 
	X.PredominantBuildingEquipmentBreakdownGroup, 
	X.PredominantBuildingPropertyCOBFactor, 
	X.PredominantLiabilityCOBFactor, 
	X.LocationBuildingNumberShadow,
	X.PredominantBuildingBCCCode,
	X.PredominantBuildingClassCodeDescription
	FROM
	WB_BP_Building X
	inner join
	cte_WBBPBuilding Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	BP_BuildingId,
	WB_BP_BuildingId,
	SessionId,
	AlarmLocal AS i_AlarmLocal,
	AlarmCentralStation AS i_AlarmCentralStation,
	SafeClassBOrBetter AS i_SafeClassBOrBetter,
	SafeOther AS i_SafeOther,
	LocksDeadbolt AS i_LocksDeadbolt,
	LocksOther AS i_LocksOther,
	ClosedCircuitTV AS i_ClosedCircuitTV,
	NoCrimeControls AS i_NoCrimeControls,
	-- *INF*: DECODE(i_AlarmLocal,'T','1','F','0',NULL)
	DECODE(
	    i_AlarmLocal,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS AlarmLocal,
	-- *INF*: DECODE(i_AlarmCentralStation,'T','1','F','0',NULL)
	DECODE(
	    i_AlarmCentralStation,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS AlarmCentralStation,
	-- *INF*: DECODE(i_SafeClassBOrBetter,'T','1','F','0',NULL)
	DECODE(
	    i_SafeClassBOrBetter,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS SafeClassBOrBetter,
	-- *INF*: DECODE(i_SafeOther,'T','1','F','0',NULL)
	DECODE(
	    i_SafeOther,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS SafeOther,
	-- *INF*: DECODE(i_LocksDeadbolt,'T','1','F','0',NULL)
	DECODE(
	    i_LocksDeadbolt,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS LocksDeadbolt,
	-- *INF*: DECODE(i_LocksOther,'T','1','F','0',NULL)
	DECODE(
	    i_LocksOther,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS LocksOther,
	-- *INF*: DECODE(i_ClosedCircuitTV,'T','1','F','0',NULL)
	DECODE(
	    i_ClosedCircuitTV,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS ClosedCircuitTV,
	-- *INF*: DECODE(i_NoCrimeControls,'T','1','F','0',NULL)
	DECODE(
	    i_NoCrimeControls,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS NoCrimeControls,
	PredominantBuildingEquipmentBreakdownGroup,
	PredominantBuildingPropertyCOBFactor,
	PredominantLiabilityCOBFactor,
	LocationBuildingNumberShadow,
	PredominantBuildingBCCCode,
	PredominantBuildingClassCodeDescription,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_WB_BP_Building
),
WBBPBuildingStage AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.WBBPBuildingStage;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.WBBPBuildingStage
	(ExtractDate, SourceSystemid, BP_BuildingId, WB_BP_BuildingId, SessionId, AlarmLocal, AlarmCentralStation, SafeClassBOrBetter, SafeOther, LocksDeadbolt, LocksOther, ClosedCircuitTV, NoCrimeControls, PredominantBuildingEquipmentBreakdownGroup, PredominantBuildingPropertyCOBFactor, PredominantLiabilityCOBFactor, LocationBuildingNumberShadow, PredominantBuildingBCCCode, PredominantBuildingClassCodeDescription)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	BP_BUILDINGID, 
	WB_BP_BUILDINGID, 
	SESSIONID, 
	ALARMLOCAL, 
	ALARMCENTRALSTATION, 
	SAFECLASSBORBETTER, 
	SAFEOTHER, 
	LOCKSDEADBOLT, 
	LOCKSOTHER, 
	CLOSEDCIRCUITTV, 
	NOCRIMECONTROLS, 
	PREDOMINANTBUILDINGEQUIPMENTBREAKDOWNGROUP, 
	PREDOMINANTBUILDINGPROPERTYCOBFACTOR, 
	PREDOMINANTLIABILITYCOBFACTOR, 
	LOCATIONBUILDINGNUMBERSHADOW, 
	PREDOMINANTBUILDINGBCCCODE, 
	PREDOMINANTBUILDINGCLASSCODEDESCRIPTION
	FROM EXP_Metadata
),