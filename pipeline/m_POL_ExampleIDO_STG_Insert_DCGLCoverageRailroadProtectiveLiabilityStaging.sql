WITH
SQ_DC_GL_CoverageRailroadProtectiveLiability AS (
	WITH cte_DCGLCoverageRailroadProtectiveLiability(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.CoverageId, 
	X.GL_CoverageRailroadProtectiveLiabilityId, 
	X.SessionId, 
	X.Auditable, 
	X.ConstructionOperations, 
	X.ExtendCoverage, 
	X.NumberOfTrains, 
	X.OperationType, 
	X.PartlyNoHazard, 
	X.PercentNoHazard, 
	X.RailroadHazardRatio, 
	X.SubClass, 
	X.WorkTrainsAssigned, 
	X.RailroadProtectiveLiability, 
	X.Exposure, 
	X.ExposureAudited, 
	X.ExposureEstimated 
	FROM
	DC_GL_CoverageRailroadProtectiveLiability X
	inner join
	cte_DCGLCoverageRailroadProtectiveLiability Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	CoverageId,
	GL_CoverageRailroadProtectiveLiabilityId,
	SessionId,
	Auditable AS i_Auditable,
	-- *INF*: IIF(i_Auditable='T','1','0')
	IFF(i_Auditable = 'T', '1', '0') AS o_Auditable,
	ConstructionOperations AS i_ConstructionOperations,
	-- *INF*: IIF(i_ConstructionOperations='T','1','0')
	IFF(i_ConstructionOperations = 'T', '1', '0') AS o_ConstructionOperations,
	ExtendCoverage AS i_ExtendCoverage,
	-- *INF*: IIF(i_ExtendCoverage='T','1','0')
	IFF(i_ExtendCoverage = 'T', '1', '0') AS o_ExtendCoverage,
	NumberOfTrains,
	OperationType,
	PartlyNoHazard AS i_PartlyNoHazard,
	-- *INF*: IIF(i_PartlyNoHazard='T','1','0')
	IFF(i_PartlyNoHazard = 'T', '1', '0') AS o_PartlyNoHazard,
	PercentNoHazard,
	RailroadHazardRatio,
	SubClass,
	WorkTrainsAssigned AS i_WorkTrainsAssigned,
	-- *INF*: IIF(i_WorkTrainsAssigned='T','1','0')
	IFF(i_WorkTrainsAssigned = 'T', '1', '0') AS o_WorkTrainsAssigned,
	RailroadProtectiveLiability,
	Exposure,
	ExposureAudited,
	ExposureEstimated,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_DC_GL_CoverageRailroadProtectiveLiability
),
DCGLCoverageRailroadProtectiveLiabilityStaging AS (
	TRUNCATE TABLE DCGLCoverageRailroadProtectiveLiabilityStaging;
	INSERT INTO DCGLCoverageRailroadProtectiveLiabilityStaging
	(CoverageId, GL_CoverageRailroadProtectiveLiabilityId, SessionId, Auditable, ConstructionOperations, ExtendCoverage, NumberOfTrains, OperationType, PartlyNoHazard, PercentNoHazard, RailroadHazardRatio, SubClass, WorkTrainsAssigned, RailroadProtectiveLiability, Exposure, ExposureAudited, ExposureEstimated, ExtractDate, SourceSystemId)
	SELECT 
	COVERAGEID, 
	GL_COVERAGERAILROADPROTECTIVELIABILITYID, 
	SESSIONID, 
	o_Auditable AS AUDITABLE, 
	o_ConstructionOperations AS CONSTRUCTIONOPERATIONS, 
	o_ExtendCoverage AS EXTENDCOVERAGE, 
	NUMBEROFTRAINS, 
	OPERATIONTYPE, 
	o_PartlyNoHazard AS PARTLYNOHAZARD, 
	PERCENTNOHAZARD, 
	RAILROADHAZARDRATIO, 
	SUBCLASS, 
	o_WorkTrainsAssigned AS WORKTRAINSASSIGNED, 
	RAILROADPROTECTIVELIABILITY, 
	EXPOSURE, 
	EXPOSUREAUDITED, 
	EXPOSUREESTIMATED, 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID
	FROM EXP_Metadata
),