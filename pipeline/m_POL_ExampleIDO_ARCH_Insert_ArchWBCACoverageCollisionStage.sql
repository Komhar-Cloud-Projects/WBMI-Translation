WITH
SQ_WBCACoverageCollisionStage AS (
	SELECT
		WBCACoverageCollisionStageId,
		ExtractDate,
		SourceSystemId,
		CA_CoverageCollisionId,
		WB_CA_CoverageCollisionId,
		SessionId,
		PremiumPrior,
		ReplacementCost
	FROM WBCACoverageCollisionStage
),
EXP_Metadata AS (
	SELECT
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	WBCACoverageCollisionStageId,
	ExtractDate,
	SourceSystemId,
	CA_CoverageCollisionId,
	WB_CA_CoverageCollisionId,
	SessionId,
	PremiumPrior,
	ReplacementCost AS i_ReplacementCost,
	-- *INF*: decode(i_ReplacementCost,'T',1,'F',0,NULL)
	-- 
	decode(
	    i_ReplacementCost,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_ReplacementCost
	FROM SQ_WBCACoverageCollisionStage
),
ArchWBCACoverageCollisionStage AS (
	INSERT INTO ArchWBCACoverageCollisionStage
	(ExtractDate, SourceSystemId, AuditId, WBCACoverageCollisionStageId, CA_CoverageCollisionId, WB_CA_CoverageCollisionId, SessionId, PremiumPrior, ReplacementCost)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
	WBCACOVERAGECOLLISIONSTAGEID, 
	CA_COVERAGECOLLISIONID, 
	WB_CA_COVERAGECOLLISIONID, 
	SESSIONID, 
	PREMIUMPRIOR, 
	o_ReplacementCost AS REPLACEMENTCOST
	FROM EXP_Metadata
),