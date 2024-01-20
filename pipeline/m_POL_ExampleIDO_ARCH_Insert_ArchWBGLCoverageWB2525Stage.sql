WITH
SQ_WBGLCoverageWB2525Stage AS (
	SELECT
		WBGLCoverageWB2525StageId,
		ExtractDate,
		SourceSystemId,
		CoverageId,
		WB_GL_CoverageWB2525Id,
		SessionId,
		BillingLOB,
		CommissionPlanId,
		IsBillingSubline,
		ParentBillingLOB,
		PurePremium,
		TransactionCommissionType,
		TransactionCommissionValue,
		TransactionFinalCommissionValue
	FROM WBGLCoverageWB2525Stage
),
EXP_Metadata AS (
	SELECT
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	WBGLCoverageWB2525StageId,
	ExtractDate,
	SourceSystemId,
	CoverageId,
	WB_GL_CoverageWB2525Id,
	SessionId,
	BillingLOB,
	CommissionPlanId,
	IsBillingSubline,
	ParentBillingLOB,
	PurePremium,
	TransactionCommissionType,
	TransactionCommissionValue,
	TransactionFinalCommissionValue
	FROM SQ_WBGLCoverageWB2525Stage
),
ArchWBGLCoverageWB2525Stage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ArchWBGLCoverageWB2525Stage
	(ExtractDate, SourceSystemId, AuditId, WBGLCoverageWB2525StageId, CoverageId, WB_GL_CoverageWB2525Id, SessionId, BillingLOB, CommissionPlanId, IsBillingSubline, ParentBillingLOB, PurePremium, TransactionCommissionType, TransactionCommissionValue, TransactionFinalCommissionValue)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
	WBGLCOVERAGEWB2525STAGEID, 
	COVERAGEID, 
	WB_GL_COVERAGEWB2525ID, 
	SESSIONID, 
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