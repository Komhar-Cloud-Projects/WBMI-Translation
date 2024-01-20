WITH
SQ_DC_CF_Builder AS (
	WITH cte_DCCFBuilder(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.CF_RiskId, 
	X.CF_BuilderId, 
	X.SessionId, 
	X.Id, 
	X.Renovations, 
	X.TheftOfBuildingMaterials, 
	X.BuildingMaterialsSuppliesOfOthersPremium, 
	X.Collapse, 
	X.SubContractors, 
	X.BuildingPremisesDescription 
	FROM
	DC_CF_Builder X
	inner join
	cte_DCCFBuilder Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	CF_RiskId,
	CF_BuilderId,
	SessionId,
	Id,
	Renovations AS i_Renovations,
	-- *INF*: DECODE(i_Renovations,'T',1,'F',0,NULL)
	DECODE(
	    i_Renovations,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_Renovations,
	TheftOfBuildingMaterials AS i_TheftOfBuildingMaterials,
	-- *INF*: DECODE(i_TheftOfBuildingMaterials,'T',1,'F',0,NULL)
	DECODE(
	    i_TheftOfBuildingMaterials,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_TheftOfBuildingMaterials,
	BuildingMaterialsSuppliesOfOthersPremium,
	Collapse,
	SubContractors,
	BuildingPremisesDescription,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_DC_CF_Builder
),
DCCFBuilderStaging AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.DCCFBuilderStaging;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.DCCFBuilderStaging
	(ExtractDate, SourceSystemId, CF_RiskId, CF_BuilderId, SessionId, Id, Renovations, TheftOfBuildingMaterials, BuildingMaterialsSuppliesOfOthersPremium, Collapse, SubContractors, BuildingPremisesDescription)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	CF_RISKID, 
	CF_BUILDERID, 
	SESSIONID, 
	ID, 
	o_Renovations AS RENOVATIONS, 
	o_TheftOfBuildingMaterials AS THEFTOFBUILDINGMATERIALS, 
	BUILDINGMATERIALSSUPPLIESOFOTHERSPREMIUM, 
	COLLAPSE, 
	SUBCONTRACTORS, 
	BUILDINGPREMISESDESCRIPTION
	FROM EXP_Metadata
),