WITH
SQ_DCBPCoverageBusinessIncomeOrdinaryPayrollStage AS (
	SELECT
		DCBPCoverageBusinessIncomeOrdinaryPayrollStageId,
		ExtractDate,
		SourceSystemId,
		CoverageId,
		BP_CoverageBusinessIncomeOrdinaryPayrollId,
		SessionId,
		Days
	FROM DCBPCoverageBusinessIncomeOrdinaryPayrollStage
),
EXP_Metadata AS (
	SELECT
	DCBPCoverageBusinessIncomeOrdinaryPayrollStageId,
	ExtractDate,
	SourceSystemId,
	CoverageId,
	BP_CoverageBusinessIncomeOrdinaryPayrollId,
	SessionId,
	Days,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM SQ_DCBPCoverageBusinessIncomeOrdinaryPayrollStage
),
ArchDCBPCoverageBusinessIncomeOrdinaryPayrollStage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ArchDCBPCoverageBusinessIncomeOrdinaryPayrollStage
	(ExtractDate, SourceSystemId, AuditId, DCBPCoverageBusinessIncomeOrdinaryPayrollStageId, CoverageId, BP_CoverageBusinessIncomeOrdinaryPayrollId, SessionId, Days)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
	DCBPCOVERAGEBUSINESSINCOMEORDINARYPAYROLLSTAGEID, 
	COVERAGEID, 
	BP_COVERAGEBUSINESSINCOMEORDINARYPAYROLLID, 
	SESSIONID, 
	DAYS
	FROM EXP_Metadata
),