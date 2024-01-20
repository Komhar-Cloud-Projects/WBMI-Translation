WITH
SQ_WB_BP_Building AS (
	SELECT
		ExtractDate,
		SourceSystemid,
		BP_BuildingId,
		WB_BP_BuildingId,
		SessionId,
		AlarmLocal,
		AlarmCentralStation,
		SafeClassBOrBetter,
		SafeOther,
		LocksDeadbolt,
		LocksOther,
		ClosedCircuitTV,
		NoCrimeControls,
		PredominantBuildingEquipmentBreakdownGroup,
		PredominantBuildingPropertyCOBFactor,
		PredominantLiabilityCOBFactor,
		LocationBuildingNumberShadow,
		PredominantBuildingBCCCode,
		PredominantBuildingClassCodeDescription
	FROM WBBPBuildingStage
),
EXP_Metadata AS (
	SELECT
	ExtractDate,
	SourceSystemid,
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
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM SQ_WB_BP_Building
),
ArchWBBPBuildingStage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ArchWBBPBuildingStage
	(ExtractDate, SourceSystemId, AuditId, BP_BuildingId, WB_BP_BuildingId, SessionId, AlarmLocal, AlarmCentralStation, SafeClassBOrBetter, SafeOther, LocksDeadbolt, LocksOther, ClosedCircuitTV, NoCrimeControls, PredominantBuildingEquipmentBreakdownGroup, PredominantBuildingPropertyCOBFactor, PredominantLiabilityCOBFactor, LocationBuildingNumberShadow, PredominantBuildingBCCCode, PredominantBuildingClassCodeDescription)
	SELECT 
	EXTRACTDATE, 
	SourceSystemid AS SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
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