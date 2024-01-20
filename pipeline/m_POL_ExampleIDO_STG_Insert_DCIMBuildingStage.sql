WITH
SQ_DC_IM_Building AS (
	WITH cte_DCIMBuilding(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.LineId, 
	X.IM_LocationId, 
	X.IM_BuildingId, 
	X.SessionId, 
	X.Id, 
	X.ConstructionCode, 
	X.Description, 
	X.DoorType, 
	X.NumberOfStories, 
	X.RoofCovering, 
	X.RoofDeckAttachment, 
	X.RoofGeometry, 
	X.RoofWallConstruction, 
	X.Sprinkler, 
	X.SquareFt, 
	X.WindowProtection, 
	X.WindstormLossMitigation, 
	X.YearBuilt, 
	X.IM_LocationXmlId 
	FROM
	DC_IM_Building X
	inner join
	cte_DCIMBuilding Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	LineId,
	IM_LocationId,
	IM_BuildingId,
	SessionId,
	Id,
	ConstructionCode,
	Description,
	DoorType,
	NumberOfStories,
	RoofCovering,
	RoofDeckAttachment,
	RoofGeometry,
	RoofWallConstruction,
	Sprinkler,
	SquareFt,
	WindowProtection,
	WindstormLossMitigation,
	YearBuilt,
	IM_LocationXmlId,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_DC_IM_Building
),
DCIMBuildingStage2 AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.DCIMBuildingStage;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.DCIMBuildingStage
	(LineId, IMLocationId, IMBuildingId, SessionId, Id, ConstructionCode, Description, DoorType, NumberOfStories, RoofCovering, RoofDeckAttachment, RoofGeometry, RoofWallConstruction, Sprinkler, SquareFt, WindowProtection, WindstormLossMitigation, YearBuilt, IMLocationXmlId, ExtractDate, SourceSystemId)
	SELECT 
	LINEID, 
	IM_LocationId AS IMLOCATIONID, 
	IM_BuildingId AS IMBUILDINGID, 
	SESSIONID, 
	ID, 
	CONSTRUCTIONCODE, 
	DESCRIPTION, 
	DOORTYPE, 
	NUMBEROFSTORIES, 
	ROOFCOVERING, 
	ROOFDECKATTACHMENT, 
	ROOFGEOMETRY, 
	ROOFWALLCONSTRUCTION, 
	SPRINKLER, 
	SQUAREFT, 
	WINDOWPROTECTION, 
	WINDSTORMLOSSMITIGATION, 
	YEARBUILT, 
	IM_LocationXmlId AS IMLOCATIONXMLID, 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID
	FROM EXP_Metadata
),