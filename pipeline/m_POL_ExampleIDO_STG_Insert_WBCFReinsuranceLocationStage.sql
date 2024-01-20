WITH
SQ_WB_CF_ReinsuranceLocation AS (
	WITH cte_WBCFReinsuranceLocation(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.WB_CF_ReinsuranceId, 
	X.WB_CF_ReinsuranceLocationId, 
	X.SessionId, 
	X.BuildingNumber, 
	X.Occupancy, 
	X.Construction, 
	X.Stories, 
	X.ProtectionClass, 
	X.YearBuilt, 
	X.AdditionalBuildingNumbers, 
	X.Sprinklered, 
	X.SprinkleredPercentage, 
	X.LocationNumber, 
	X.LocationAddress 
	FROM
	WB_CF_ReinsuranceLocation X
	inner join
	cte_WBCFReinsuranceLocation Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	WB_CF_ReinsuranceId,
	WB_CF_ReinsuranceLocationId,
	SessionId,
	BuildingNumber,
	Occupancy,
	Construction,
	Stories,
	ProtectionClass,
	YearBuilt,
	AdditionalBuildingNumbers,
	Sprinklered AS i_Sprinklered,
	-- *INF*: DECODE(i_Sprinklered, 'T', 1, 'F', 0, NULL)
	DECODE(
	    i_Sprinklered,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS Sprinklered,
	SprinkleredPercentage,
	LocationNumber,
	LocationAddress,
	sysdate AS ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS SourceSystemId
	FROM SQ_WB_CF_ReinsuranceLocation
),
WBCFReinsuranceLocationStage AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.WBCFReinsuranceLocationStage;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.WBCFReinsuranceLocationStage
	(ExtractDate, SourceSystemId, WBCFReinsuranceId, WBCFReinsuranceLocationId, SessionId, BuildingNumber, Occupancy, Construction, Stories, ProtectionClass, YearBuilt, AdditionalBuildingNumbers, Sprinklered, SprinkleredPercentage, LocationNumber, LocationAddress)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	WB_CF_ReinsuranceId AS WBCFREINSURANCEID, 
	WB_CF_ReinsuranceLocationId AS WBCFREINSURANCELOCATIONID, 
	SESSIONID, 
	BUILDINGNUMBER, 
	OCCUPANCY, 
	CONSTRUCTION, 
	STORIES, 
	PROTECTIONCLASS, 
	YEARBUILT, 
	ADDITIONALBUILDINGNUMBERS, 
	SPRINKLERED, 
	SPRINKLEREDPERCENTAGE, 
	LOCATIONNUMBER, 
	LOCATIONADDRESS
	FROM EXP_Metadata
),