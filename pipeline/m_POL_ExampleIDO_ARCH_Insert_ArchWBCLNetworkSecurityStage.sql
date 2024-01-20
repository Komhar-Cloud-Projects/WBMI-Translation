WITH
WBCLCoverageNetworkSecurityStage AS (
	SELECT
		WBCLCoverageNetworkSecurityStageId,
		ExtractDate,
		SourceSystemid,
		CoverageId,
		WB_CL_CoverageNetworkSecurityId,
		SessionId,
		Selected,
		ThirdPartyBusiness,
		BillingLOB,
		CommissionPlanID,
		IsBillingSubline,
		ParentBillingLOB,
		TransactionFinalCommissionValue
	FROM WBCLCoverageNetworkSecurityStage
	INNER JOIN WBCLCoverageNetworkSecurityStage
),
EXP_Metadata AS (
	SELECT
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	WBCLCoverageNetworkSecurityStageId AS WBCLCoverageExtortionStageId,
	ExtractDate,
	SourceSystemid,
	CoverageId,
	WB_CL_CoverageNetworkSecurityId,
	SessionId,
	Selected,
	ThirdPartyBusiness,
	BillingLOB,
	CommissionPlanID,
	IsBillingSubline,
	ParentBillingLOB,
	TransactionFinalCommissionValue
	FROM WBCLCoverageNetworkSecurityStage
),
Shortcut_to_ArchWBCLCoverageNetworkSecurityStage AS (
	INSERT INTO ArchWBCLCoverageNetworkSecurityStage
	(ExtractDate, SourceSystemId, AuditId, WBCLCoverageNetworkSecurityStageId, CoverageId, WB_CL_CoverageNetworkSecurityId, SessionId, Selected, ThirdPartyBusiness, BillingLOB, CommissionPlanID, IsBillingSubline, ParentBillingLOB, TransactionFinalCommissionValue)
	SELECT 
	EXTRACTDATE, 
	SourceSystemid AS SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
	WBCLCoverageExtortionStageId AS WBCLCOVERAGENETWORKSECURITYSTAGEID, 
	COVERAGEID, 
	WB_CL_COVERAGENETWORKSECURITYID, 
	SESSIONID, 
	SELECTED, 
	THIRDPARTYBUSINESS, 
	BILLINGLOB, 
	COMMISSIONPLANID, 
	ISBILLINGSUBLINE, 
	PARENTBILLINGLOB, 
	TRANSACTIONFINALCOMMISSIONVALUE
	FROM EXP_Metadata
),