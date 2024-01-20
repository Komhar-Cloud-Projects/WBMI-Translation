WITH
SQ_WBBPCoverageEmploymentPracticesLiabilityStage AS (
	SELECT
		WBBPCoverageEmploymentPracticesLiabilityStageId,
		ExtractDate,
		SourceSystemId,
		CoverageId,
		WB_BP_CoverageEmploymentPracticesLiabilityId,
		SessionId,
		NumberOfEmployees,
		RetroactiveDate,
		PriorLosses,
		BillingLOB,
		CommissionPlanID,
		IsBillingSubline,
		ParentBillingLOB,
		PurePremium,
		TransactionCommissionType,
		TransactionCommissionValue,
		TransactionFinalCommissionValue
	FROM WBBPCoverageEmploymentPracticesLiabilityStage
),
EXP_Metadata AS (
	SELECT
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	WBBPCoverageEmploymentPracticesLiabilityStageId,
	ExtractDate,
	SourceSystemId,
	CoverageId,
	WB_BP_CoverageEmploymentPracticesLiabilityId,
	SessionId,
	NumberOfEmployees,
	RetroactiveDate,
	PriorLosses,
	BillingLOB,
	CommissionPlanID,
	IsBillingSubline,
	-- *INF*: DECODE(IsBillingSubline,'T',1,'F',0,NULL)
	DECODE(
	    IsBillingSubline,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_IsBillingSubline,
	ParentBillingLOB,
	PurePremium,
	TransactionCommissionType,
	TransactionCommissionValue,
	TransactionFinalCommissionValue
	FROM SQ_WBBPCoverageEmploymentPracticesLiabilityStage
),
ArchWBBPCoverageEmploymentPracticesLiabilityStage AS (
	INSERT INTO ArchWBBPCoverageEmploymentPracticesLiabilityStage
	(ExtractDate, SourceSystemId, AuditId, WBBPCoverageEmploymentPracticesLiabilityStageId, CoverageId, WB_BP_CoverageEmploymentPracticesLiabilityId, SessionId, NumberOfEmployees, RetroactiveDate, PriorLosses, BillingLOB, CommissionPlanID, IsBillingSubline, ParentBillingLOB, PurePremium, TransactionCommissionType, TransactionCommissionValue, TransactionFinalCommissionValue)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
	WBBPCOVERAGEEMPLOYMENTPRACTICESLIABILITYSTAGEID, 
	COVERAGEID, 
	WB_BP_COVERAGEEMPLOYMENTPRACTICESLIABILITYID, 
	SESSIONID, 
	NUMBEROFEMPLOYEES, 
	RETROACTIVEDATE, 
	PRIORLOSSES, 
	BILLINGLOB, 
	COMMISSIONPLANID, 
	o_IsBillingSubline AS ISBILLINGSUBLINE, 
	PARENTBILLINGLOB, 
	PUREPREMIUM, 
	TRANSACTIONCOMMISSIONTYPE, 
	TRANSACTIONCOMMISSIONVALUE, 
	TRANSACTIONFINALCOMMISSIONVALUE
	FROM EXP_Metadata
),