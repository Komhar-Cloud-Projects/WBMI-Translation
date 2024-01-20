WITH
SQ_WBECCoverageTRIAStage AS (
	SELECT
		WBECCoverageTRIAStageId,
		ExtractDate,
		SourceSystemId,
		CoverageId,
		WB_EC_CoverageTRIAId,
		SessionId
	FROM WBECCoverageTRIAStage
),
EXP_Metadata AS (
	SELECT
	WBECCoverageTRIAStageId,
	ExtractDate,
	SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	CoverageId,
	WB_EC_CoverageTRIAId,
	SessionId
	FROM SQ_WBECCoverageTRIAStage
),
ArchWBECCoverageTRIAStage AS (
	INSERT INTO ArchWBECCoverageTRIAStage
	(ExtractDate, SourceSystemId, AuditId, WBECCoverageTRIAStageId, CoverageId, WB_EC_CoverageTRIAId, SessionId)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
	WBECCOVERAGETRIASTAGEID, 
	COVERAGEID, 
	WB_EC_COVERAGETRIAID, 
	SESSIONID
	FROM EXP_Metadata
),