WITH
SQ_WBWCCoverageTermStage AS (
	SELECT
		WBWCCoverageTermStageId,
		CoverageId,
		WB_CoverageId,
		WB_WC_CoverageTermId,
		SessionId,
		PeriodStartDate,
		PeriodEndDate,
		TermRateEffectivedate,
		TermType,
		ExtractDate,
		SourceSystemId
	FROM WBWCCoverageTermStage
),
EXP_Metadata AS (
	SELECT
	ExtractDate,
	SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	WBWCCoverageTermStageId,
	CoverageId,
	WB_CoverageId,
	WB_WC_CoverageTermId,
	SessionId,
	PeriodStartDate,
	PeriodEndDate,
	TermRateEffectivedate,
	TermType
	FROM SQ_WBWCCoverageTermStage
),
ArchWBWCCoverageTermStage AS (
	INSERT INTO ArchWBWCCoverageTermStage
	(ExtractDate, SourceSystemId, AuditId, WBWCCoverageTermStageId, CoverageId, WB_CoverageId, WB_WC_CoverageTermId, SessionId, PeriodStartDate, PeriodEndDate, TermRateEffectivedate, TermType)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
	WBWCCOVERAGETERMSTAGEID, 
	COVERAGEID, 
	WB_COVERAGEID, 
	WB_WC_COVERAGETERMID, 
	SESSIONID, 
	PERIODSTARTDATE, 
	PERIODENDDATE, 
	TERMRATEEFFECTIVEDATE, 
	TERMTYPE
	FROM EXP_Metadata
),