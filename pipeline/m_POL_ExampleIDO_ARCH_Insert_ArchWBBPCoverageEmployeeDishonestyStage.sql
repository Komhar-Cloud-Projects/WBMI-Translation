WITH
SQ_WBBPCoverageEmployeeDishonestyStage AS (
	SELECT
		WBBPCoverageEmployeeDishonestyStageId,
		ExtractDate,
		SourceSystemid,
		BP_CoverageEmployeeDishonestyId,
		WB_BP_CoverageEmployeeDishonestyId,
		SessionId,
		ERISAPlanName,
		AuditConducted,
		WhoPerformsAudit,
		AuditRenderedTo,
		BankAccountsReconciled,
		CountersignatureRequired,
		SecuritiesJointControl,
		VacationRequired,
		DesignatedAgentsAsEmployees
	FROM WBBPCoverageEmployeeDishonestyStage
),
EXP_TRANS AS (
	SELECT
	WBBPCoverageEmployeeDishonestyStageId,
	ExtractDate,
	SourceSystemid,
	BP_CoverageEmployeeDishonestyId,
	WB_BP_CoverageEmployeeDishonestyId,
	SessionId,
	ERISAPlanName,
	AuditConducted,
	WhoPerformsAudit,
	AuditRenderedTo,
	BankAccountsReconciled,
	CountersignatureRequired,
	SecuritiesJointControl,
	VacationRequired,
	DesignatedAgentsAsEmployees,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM SQ_WBBPCoverageEmployeeDishonestyStage
),
ArchWBBPCoverageEmployeeDishonestyStage AS (
	INSERT INTO ArchWBBPCoverageEmployeeDishonestyStage
	(ExtractDate, SourceSystemId, AuditId, WBBPCoverageEmployeeDishonestyStageId, BP_CoverageEmployeeDishonestyId, WB_BP_CoverageEmployeeDishonestyId, SessionId, ERISAPlanName, AuditConducted, WhoPerformsAudit, AuditRenderedTo, BankAccountsReconciled, CountersignatureRequired, SecuritiesJointControl, VacationRequired, DesignatedAgentsAsEmployees)
	SELECT 
	EXTRACTDATE, 
	SourceSystemid AS SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
	WBBPCOVERAGEEMPLOYEEDISHONESTYSTAGEID, 
	BP_COVERAGEEMPLOYEEDISHONESTYID, 
	WB_BP_COVERAGEEMPLOYEEDISHONESTYID, 
	SESSIONID, 
	ERISAPLANNAME, 
	AUDITCONDUCTED, 
	WHOPERFORMSAUDIT, 
	AUDITRENDEREDTO, 
	BANKACCOUNTSRECONCILED, 
	COUNTERSIGNATUREREQUIRED, 
	SECURITIESJOINTCONTROL, 
	VACATIONREQUIRED, 
	DESIGNATEDAGENTSASEMPLOYEES
	FROM EXP_TRANS
),