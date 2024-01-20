WITH
SQ_WBCFReinsuranceLocationStage AS (
	SELECT
		WBCFReinsuranceLocationStageId,
		ExtractDate,
		SourceSystemId,
		WBCFReinsuranceId,
		WBCFReinsuranceLocationId,
		SessionId,
		BuildingNumber,
		Occupancy,
		Construction,
		Stories,
		ProtectionClass,
		YearBuilt,
		AdditionalBuildingNumbers,
		Sprinklered,
		SprinkleredPercentage,
		LocationNumber,
		LocationAddress
	FROM WBCFReinsuranceLocationStage
),
EXP_Metadata AS (
	SELECT
	WBCFReinsuranceLocationStageId,
	ExtractDate,
	SourceSystemId,
	WBCFReinsuranceId,
	WBCFReinsuranceLocationId,
	SessionId,
	BuildingNumber,
	Occupancy,
	Construction,
	Stories,
	ProtectionClass,
	YearBuilt,
	AdditionalBuildingNumbers,
	Sprinklered,
	-- *INF*: DECODE(Sprinklered, 'T', 1, 'F', 0, NULL)
	DECODE(
	    Sprinklered,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_Sprinklered,
	SprinkleredPercentage,
	LocationNumber,
	LocationAddress,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditId
	FROM SQ_WBCFReinsuranceLocationStage
),
ArchWBCFReinsuranceLocationStage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ArchWBCFReinsuranceLocationStage
	(ExtractDate, SourceSystemId, AuditId, WBCFReinsuranceLocationStageId, WBCFReinsuranceId, WBCFReinsuranceLocationId, SessionId, BuildingNumber, Occupancy, Construction, Stories, ProtectionClass, YearBuilt, AdditionalBuildingNumbers, Sprinklered, SprinkleredPercentage, LocationNumber, LocationAddress)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	AUDITID, 
	WBCFREINSURANCELOCATIONSTAGEID, 
	WBCFREINSURANCEID, 
	WBCFREINSURANCELOCATIONID, 
	SESSIONID, 
	BUILDINGNUMBER, 
	OCCUPANCY, 
	CONSTRUCTION, 
	STORIES, 
	PROTECTIONCLASS, 
	YEARBUILT, 
	ADDITIONALBUILDINGNUMBERS, 
	o_Sprinklered AS SPRINKLERED, 
	SPRINKLEREDPERCENTAGE, 
	LOCATIONNUMBER, 
	LOCATIONADDRESS
	FROM EXP_Metadata
),