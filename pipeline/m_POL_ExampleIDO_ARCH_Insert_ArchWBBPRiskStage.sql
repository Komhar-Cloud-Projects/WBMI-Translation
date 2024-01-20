WITH
SQ_WBBPRiskStage AS (
	SELECT
		WBBPRiskStageId AS WBBPRiskStageID,
		ExtractDate,
		SourceSystemId,
		BP_RiskId,
		WB_BP_RiskId,
		SessionId,
		FunctionalValuationReason,
		DescribeOther,
		VacantBuilding,
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
		IncludesEarthquakeBuilding,
		IncludesEarthquakePersonalProperty,
		BlanketType
	FROM WBBPRiskStage
),
EXPTRANS AS (
	SELECT
	WBBPRiskStageID,
	ExtractDate,
	SourceSystemId,
	BP_RiskId,
	WB_BP_RiskId,
	SessionId,
	FunctionalValuationReason,
	DescribeOther,
	VacantBuilding,
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
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM SQ_WBBPRiskStage
),
ArchWBBPRiskStage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ArchWBBPRiskStage
	(ExtractDate, SourceSystemId, AuditId, WBBPRiskStageId, BP_RiskId, WB_BP_RiskId, SessionId, FunctionalValuationReason, DescribeOther, VacantBuilding, BlanketBuildingIneligible, BlanketPersonalPropertyGroupID, IncludesEarthquakeBuilding, IncludesEarthquakePersonalProperty, BlanketType, Message1, Message2, Message3, SumOfLimits, LocationID, BuildingID, BuildingNumber, ProtectionClassOverride)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
	WBBPRiskStageID AS WBBPRISKSTAGEID, 
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