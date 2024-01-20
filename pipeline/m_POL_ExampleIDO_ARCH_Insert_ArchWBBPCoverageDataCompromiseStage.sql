WITH
SQ_WBBPCoverageDataCompromiseStage AS (
	SELECT
		WBBPCoverageDataCompromiseStageId,
		ExtractDate,
		SourceSystemId,
		CoverageId,
		WB_BP_CoverageDataCompromiseId,
		SessionId,
		ResponseExpensesTotalPremium,
		DefenseAndLiabilityTotalPremium,
		EachSuitDataCompromiseStaticText,
		BillingLOB,
		CommissionPlanID,
		IsBillingSubline,
		ParentBillingLOB,
		PurePremium,
		TransactionCommissionType,
		TransactionCommissionValue,
		TransactionFinalCommissionValue,
		WB_CL_CoverageDataCompromiseId
	FROM WBBPCoverageDataCompromiseStage
),
EXP_Metadata AS (
	SELECT
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	WBBPCoverageDataCompromiseStageId,
	ExtractDate,
	SourceSystemId,
	CoverageId,
	WB_BP_CoverageDataCompromiseId,
	SessionId,
	ResponseExpensesTotalPremium,
	DefenseAndLiabilityTotalPremium,
	EachSuitDataCompromiseStaticText,
	BillingLOB,
	CommissionPlanID,
	IsBillingSubline,
	-- *INF*: DECODE(IsBillingSubline, 'T',1,'F',0,NULL)
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
	TransactionFinalCommissionValue,
	WB_CL_CoverageDataCompromiseId
	FROM SQ_WBBPCoverageDataCompromiseStage
),
ArchWBBPCoverageDataCompromiseStage AS (
	INSERT INTO ArchWBBPCoverageDataCompromiseStage
	(ExtractDate, SourceSystemId, AuditId, WBBPCoverageDataCompromiseStageId, CoverageId, WB_BP_CoverageDataCompromiseId, SessionId, ResponseExpensesTotalPremium, DefenseAndLiabilityTotalPremium, EachSuitDataCompromiseStaticText, BillingLOB, CommissionPlanID, IsBillingSubline, ParentBillingLOB, PurePremium, TransactionCommissionType, TransactionCommissionValue, TransactionFinalCommissionValue, WB_CL_CoverageDataCompromiseId)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
	WBBPCOVERAGEDATACOMPROMISESTAGEID, 
	COVERAGEID, 
	WB_BP_COVERAGEDATACOMPROMISEID, 
	SESSIONID, 
	RESPONSEEXPENSESTOTALPREMIUM, 
	DEFENSEANDLIABILITYTOTALPREMIUM, 
	EACHSUITDATACOMPROMISESTATICTEXT, 
	BILLINGLOB, 
	COMMISSIONPLANID, 
	o_IsBillingSubline AS ISBILLINGSUBLINE, 
	PARENTBILLINGLOB, 
	PUREPREMIUM, 
	TRANSACTIONCOMMISSIONTYPE, 
	TRANSACTIONCOMMISSIONVALUE, 
	TRANSACTIONFINALCOMMISSIONVALUE, 
	WB_CL_COVERAGEDATACOMPROMISEID
	FROM EXP_Metadata
),