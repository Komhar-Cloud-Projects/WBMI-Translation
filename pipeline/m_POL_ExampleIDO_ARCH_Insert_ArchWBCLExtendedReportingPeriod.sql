WITH
WBCLCoverageExtendedReportingPeriodStage AS (
	SELECT
		WBCLCoverageExtendedReportingPeriodStageId,
		ExtractDate,
		SourceSystemid,
		CoverageId,
		WB_CL_CoverageExtendedReportingPeriodId,
		SessionId,
		EffectiveDate,
		ExpirationDate,
		BillingLOB,
		CommissionPlanID,
		IsBillingSubline,
		ParentBillingLOB,
		TransactionFinalCommissionValue
	FROM WBCLCoverageExtendedReportingPeriodStage
	INNER JOIN WBCLCoverageExtendedReportingPeriodStage
),
EXP_Metadata AS (
	SELECT
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	WBCLCoverageExtendedReportingPeriodStageId,
	ExtractDate,
	SourceSystemid,
	CoverageId,
	WB_CL_CoverageExtendedReportingPeriodId,
	SessionId,
	EffectiveDate,
	ExpirationDate,
	BillingLOB,
	CommissionPlanID,
	IsBillingSubline,
	ParentBillingLOB,
	TransactionFinalCommissionValue
	FROM WBCLCoverageExtendedReportingPeriodStage
),
ArchWBCLCoverageExtendedReportingPeriodStage AS (
	INSERT INTO ArchWBCLCoverageExtendedReportingPeriodStage
	(ExtractDate, SourceSystemId, AuditId, WBCLCoverageExtendedReportingPeriodStageId, CoverageId, WB_CL_CoverageExtendedReportingPeriodId, SessionId, EffectiveDate, ExpirationDate, BillingLOB, CommissionPlanID, IsBillingSubline, ParentBillingLOB, TransactionFinalCommissionValue)
	SELECT 
	EXTRACTDATE, 
	SourceSystemid AS SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
	WBCLCOVERAGEEXTENDEDREPORTINGPERIODSTAGEID, 
	COVERAGEID, 
	WB_CL_COVERAGEEXTENDEDREPORTINGPERIODID, 
	SESSIONID, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	BILLINGLOB, 
	COMMISSIONPLANID, 
	ISBILLINGSUBLINE, 
	PARENTBILLINGLOB, 
	TRANSACTIONFINALCOMMISSIONVALUE
	FROM EXP_Metadata
),