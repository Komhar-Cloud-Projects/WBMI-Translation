WITH
SQ_DCGLCoverageRailroadProtectiveLiabilityStaging AS (
	SELECT
		CoverageId,
		GL_CoverageRailroadProtectiveLiabilityId,
		SessionId,
		Auditable,
		ConstructionOperations,
		ExtendCoverage,
		NumberOfTrains,
		OperationType,
		PartlyNoHazard,
		PercentNoHazard,
		RailroadHazardRatio,
		SubClass,
		WorkTrainsAssigned,
		RailroadProtectiveLiability,
		Exposure,
		ExposureAudited,
		ExposureEstimated,
		ExtractDate,
		SourceSystemId
	FROM DCGLCoverageRailroadProtectiveLiabilityStaging
),
EXPTRANS AS (
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
	ExtractDate,
	SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditId
	FROM SQ_DCGLCoverageRailroadProtectiveLiabilityStaging
),
archDCGLCoverageRailroadProtectiveLiabilityStaging AS (
	INSERT INTO archDCGLCoverageRailroadProtectiveLiabilityStaging
	(CoverageId, GL_CoverageRailroadProtectiveLiabilityId, SessionId, Auditable, ConstructionOperations, ExtendCoverage, NumberOfTrains, OperationType, PartlyNoHazard, PercentNoHazard, RailroadHazardRatio, SubClass, WorkTrainsAssigned, RailroadProtectiveLiability, Exposure, ExposureAudited, ExposureEstimated, ExtractDate, SourceSystemId, AuditId)
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
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	AUDITID
	FROM EXPTRANS
),