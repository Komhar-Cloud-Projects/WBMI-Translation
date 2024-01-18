WITH
SQ_DCBPCoveragePersonalPropertyStage AS (
	SELECT
		DCBPCoveragePersonalPropertyStageId,
		ExtractDate,
		SourceSystemId,
		CoverageId,
		BP_CoveragePersonalPropertyId,
		SessionId,
		BlanketGroup,
		BlanketPremium
	FROM DCBPCoveragePersonalPropertyStage
),
EXP_Metadata AS (
	SELECT
	DCBPCoveragePersonalPropertyStageId,
	ExtractDate,
	SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	CoverageId,
	BP_CoveragePersonalPropertyId,
	SessionId,
	BlanketGroup,
	BlanketPremium
	FROM SQ_DCBPCoveragePersonalPropertyStage
),
ArchDCBPCoveragePersonalPropertyStage AS (
	INSERT INTO ArchDCBPCoveragePersonalPropertyStage
	(ExtractDate, SourceSystemId, AuditId, DCBPCoveragePersonalPropertyStageId, CoverageId, BP_CoveragePersonalPropertyId, SessionId, BlanketGroup, BlanketPremium)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
	DCBPCOVERAGEPERSONALPROPERTYSTAGEID, 
	COVERAGEID, 
	BP_COVERAGEPERSONALPROPERTYID, 
	SESSIONID, 
	BLANKETGROUP, 
	BLANKETPREMIUM
	FROM EXP_Metadata
),