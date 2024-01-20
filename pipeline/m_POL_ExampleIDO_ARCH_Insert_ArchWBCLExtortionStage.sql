WITH
WBCLCoverageExtortionStage AS (
	SELECT
		WBCLCoverageExtortionStageId,
		ExtractDate,
		SourceSystemid,
		CoverageId,
		WB_CL_CoverageExtortionId,
		SessionId,
		BillingLOB,
		CommissionPlanID,
		IsBillingSubline,
		ParentBillingLOB,
		TransactionFinalCommissionValue
	FROM WBCLCoverageExtortionStage
	INNER JOIN WBCLCoverageExtortionStage
),
EXP_Metadata AS (
	SELECT
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	WBCLCoverageExtortionStageId,
	ExtractDate,
	SourceSystemid,
	CoverageId,
	WB_CL_CoverageExtortionId,
	SessionId,
	BillingLOB,
	CommissionPlanID,
	IsBillingSubline,
	ParentBillingLOB,
	TransactionFinalCommissionValue
	FROM WBCLCoverageExtortionStage
),
ArchWBCLCoverageExtortionStage AS (
	INSERT INTO ArchWBCLCoverageExtortionStage
	(ExtractDate, SourceSystemId, AuditId, WBCLCoverageExtortionStageId, CoverageId, WB_CL_CoverageExtortionId, SessionId, BillingLOB, CommissionPlanID, IsBillingSubline, ParentBillingLOB, TransactionFinalCommissionValue)
	SELECT 
	EXTRACTDATE, 
	SourceSystemid AS SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
	WBCLCOVERAGEEXTORTIONSTAGEID, 
	COVERAGEID, 
	WB_CL_COVERAGEEXTORTIONID, 
	SESSIONID, 
	BILLINGLOB, 
	COMMISSIONPLANID, 
	ISBILLINGSUBLINE, 
	PARENTBILLINGLOB, 
	TRANSACTIONFINALCOMMISSIONVALUE
	FROM EXP_Metadata
),