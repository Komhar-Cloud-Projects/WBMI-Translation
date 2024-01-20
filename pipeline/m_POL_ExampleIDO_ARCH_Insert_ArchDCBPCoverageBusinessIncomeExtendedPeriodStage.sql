WITH
SQ_DCBPCoverageBusinessIncomeExtendedPeriodStage AS (
	SELECT
		DCBPCoverageBusinessIncomeExtendedPeriodStageId,
		ExtractDate,
		SourceSystemId,
		CoverageId,
		BP_CoverageBusinessIncomeExtendedPeriodId,
		SessionId,
		Days
	FROM DCBPCoverageBusinessIncomeExtendedPeriodStage
),
EXP_Metadata AS (
	SELECT
	DCBPCoverageBusinessIncomeExtendedPeriodStageId,
	ExtractDate,
	SourceSystemId,
	CoverageId,
	BP_CoverageBusinessIncomeExtendedPeriodId,
	SessionId,
	Days,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM SQ_DCBPCoverageBusinessIncomeExtendedPeriodStage
),
ArchDCBPCoverageBusinessIncomeExtendedPeriodStage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ArchDCBPCoverageBusinessIncomeExtendedPeriodStage
	(ExtractDate, SourceSystemId, AuditId, DCBPCoverageBusinessIncomeExtendedPeriodStageId, CoverageId, BP_CoverageBusinessIncomeExtendedPeriodId, SessionId, Days)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
	DCBPCOVERAGEBUSINESSINCOMEEXTENDEDPERIODSTAGEID, 
	COVERAGEID, 
	BP_COVERAGEBUSINESSINCOMEEXTENDEDPERIODID, 
	SESSIONID, 
	DAYS
	FROM EXP_Metadata
),