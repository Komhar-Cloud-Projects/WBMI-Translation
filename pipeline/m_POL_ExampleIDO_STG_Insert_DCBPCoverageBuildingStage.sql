WITH
SQ_DC_BP_CoverageBuilding AS (
	WITH cte_DCBPCoverageBuilding(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.CoverageId, 
	X.BP_CoverageBuildingId, 
	X.SessionId, 
	X.BlanketPremium, 
	X.RoofSurfacingLimitations 
	FROM
	 DC_BP_CoverageBuilding X
	inner join
	cte_DCBPCoverageBuilding Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	CoverageId,
	BP_CoverageBuildingId,
	SessionId,
	BlanketPremium,
	RoofSurfacingLimitations
	FROM SQ_DC_BP_CoverageBuilding
),
DCBPCoverageBuildingStage AS (
	TRUNCATE TABLE DCBPCoverageBuildingStage;
	INSERT INTO DCBPCoverageBuildingStage
	(ExtractDate, SourceSystemId, CoverageId, BP_CoverageBuildingId, SessionId, BlanketPremium, RoofSurfacingLimitations)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	COVERAGEID, 
	BP_COVERAGEBUILDINGID, 
	SESSIONID, 
	BLANKETPREMIUM, 
	ROOFSURFACINGLIMITATIONS
	FROM EXP_Metadata
),