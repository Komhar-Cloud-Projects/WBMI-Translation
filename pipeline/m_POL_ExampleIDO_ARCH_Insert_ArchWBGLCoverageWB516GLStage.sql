WITH
SQ_WBGLCoverageWB516GLStage AS (
	SELECT
		WBGLCoverageWB516GLStageId,
		ExtractDate,
		SourceSystemId,
		CoverageId,
		WB_GL_CoverageWB516GLId,
		SessionId,
		Deductible,
		RetroactiveDate,
		NumberOfEmployees,
		BillingLOB,
		CommissionPlanId,
		IsBillingSubline,
		ParentBillingLOB,
		PurePremium,
		TransactionCommissionType,
		TransactionCommissionValue,
		TransactionFinalCommissionValue
	FROM WBGLCoverageWB516GLStage
),
EXP_Metadata AS (
	SELECT
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	WBGLCoverageWB516GLStageId,
	ExtractDate,
	SourceSystemId,
	CoverageId,
	WB_GL_CoverageWB516GLId,
	SessionId,
	Deductible,
	RetroactiveDate,
	NumberOfEmployees,
	BillingLOB,
	CommissionPlanId,
	IsBillingSubline,
	ParentBillingLOB,
	PurePremium,
	TransactionCommissionType,
	TransactionCommissionValue,
	TransactionFinalCommissionValue
	FROM SQ_WBGLCoverageWB516GLStage
),
ArchWBGLCoverageWB516GLStage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ArchWBGLCoverageWB516GLStage
	(ExtractDate, SourceSystemId, AuditId, WBGLCoverageWB516GLStageId, CoverageId, WB_GL_CoverageWB516GLId, SessionId, Deductible, RetroactiveDate, NumberOfEmployees, BillingLOB, CommissionPlanId, IsBillingSubline, ParentBillingLOB, PurePremium, TransactionCommissionType, TransactionCommissionValue, TransactionFinalCommissionValue)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
	WBGLCOVERAGEWB516GLSTAGEID, 
	COVERAGEID, 
	WB_GL_COVERAGEWB516GLID, 
	SESSIONID, 
	DEDUCTIBLE, 
	RETROACTIVEDATE, 
	NUMBEROFEMPLOYEES, 
	BILLINGLOB, 
	COMMISSIONPLANID, 
	ISBILLINGSUBLINE, 
	PARENTBILLINGLOB, 
	PUREPREMIUM, 
	TRANSACTIONCOMMISSIONTYPE, 
	TRANSACTIONCOMMISSIONVALUE, 
	TRANSACTIONFINALCOMMISSIONVALUE
	FROM EXP_Metadata
),