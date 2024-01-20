WITH
SQ_DCBPCoverageBuildingStage AS (
	SELECT
		DCBPCoverageBuildingStageId,
		ExtractDate,
		SourceSystemId,
		CoverageId,
		BP_CoverageBuildingId,
		SessionId,
		BlanketPremium,
		RoofSurfacingLimitations
	FROM DCBPCoverageBuildingStage
),
EXP_Metadata AS (
	SELECT
	DCBPCoverageBuildingStageId,
	ExtractDate,
	SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	CoverageId,
	BP_CoverageBuildingId,
	SessionId,
	BlanketPremium,
	RoofSurfacingLimitations
	FROM SQ_DCBPCoverageBuildingStage
),
ArchDCBPCoverageBuildingStage AS (
	INSERT INTO ArchDCBPCoverageBuildingStage
	(ExtractDate, SourceSystemId, AuditId, DCBPCoverageBuildingStageId, CoverageId, BP_CoverageBuildingId, SessionId, BlanketPremium, RoofSurfacingLimitations)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
	DCBPCOVERAGEBUILDINGSTAGEID, 
	COVERAGEID, 
	BP_COVERAGEBUILDINGID, 
	SESSIONID, 
	BLANKETPREMIUM, 
	ROOFSURFACINGLIMITATIONS
	FROM EXP_Metadata
),