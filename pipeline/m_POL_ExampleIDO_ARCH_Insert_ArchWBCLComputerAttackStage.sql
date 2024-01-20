WITH
WBCLComputerAttackStage AS (
	SELECT
		WBCLComputerAttackStageId,
		ExtractDate,
		SourceSystemid,
		CoverageId,
		WB_CL_CoverageComputerAttackId,
		SessionId,
		Selected,
		BillingLOB,
		CommissionPlanID,
		IsBillingSubline,
		ParentBillingLOB,
		TransactionFinalCommissionValue
	FROM WBCLComputerAttackStage
	INNER JOIN WBCLComputerAttackStage
),
EXP_Metadata AS (
	SELECT
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	WBCLComputerAttackStageId,
	ExtractDate,
	SourceSystemid AS SourceSystemId,
	CoverageId,
	WB_CL_CoverageComputerAttackId,
	SessionId,
	Selected,
	BillingLOB,
	CommissionPlanID,
	IsBillingSubline,
	ParentBillingLOB,
	TransactionFinalCommissionValue
	FROM WBCLComputerAttackStage
),
ArchWBCLComputerAttackStage AS (
	INSERT INTO ArchWBCLComputerAttackStage
	(ExtractDate, SourceSystemId, AuditId, WBCLComputerAttackStageId, CoverageId, WB_CL_CoverageComputerAttackId, SessionId, Selected, BillingLOB, CommissionPlanID, IsBillingSubline, ParentBillingLOB, TransactionFinalCommissionValue)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
	WBCLCOMPUTERATTACKSTAGEID, 
	COVERAGEID, 
	WB_CL_COVERAGECOMPUTERATTACKID, 
	SESSIONID, 
	SELECTED, 
	BILLINGLOB, 
	COMMISSIONPLANID, 
	ISBILLINGSUBLINE, 
	PARENTBILLINGLOB, 
	TRANSACTIONFINALCOMMISSIONVALUE
	FROM EXP_Metadata
),