WITH
SQ_DC_CR_BuildingCoverage AS (
	WITH cte_DCCRBuildingCoverage(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.CoverageId, 
	X.CR_BuildingId, 
	X.CR_BuildingCoverageId, 
	X.SessionId, 
	X.CoverageXmlId, 
	X.CR_BuildingXmlId 
	FROM
	DC_CR_BuildingCoverage X
	inner join
	cte_DCCRBuildingCoverage Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	SYSDATE AS ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS SourceSystemId,
	CoverageId,
	CR_BuildingId,
	CR_BuildingCoverageId,
	SessionId,
	CoverageXmlId,
	CR_BuildingXmlId
	FROM SQ_DC_CR_BuildingCoverage
),
DCCRBuildingCoverageStaging AS (
	TRUNCATE TABLE DCCRBuildingCoverageStaging;
	INSERT INTO DCCRBuildingCoverageStaging
	(ExtractDate, SourceSystemId, CoverageId, CR_BuildingId, CR_BuildingCoverageId, SessionId, CoverageXmlId, CR_BuildingXmlId)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	COVERAGEID, 
	CR_BUILDINGID, 
	CR_BUILDINGCOVERAGEID, 
	SESSIONID, 
	COVERAGEXMLID, 
	CR_BUILDINGXMLID
	FROM EXP_Metadata
),