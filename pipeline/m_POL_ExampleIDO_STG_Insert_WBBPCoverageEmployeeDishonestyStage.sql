WITH
SQ_WB_BP_CoverageEmployeeDishonesty AS (
	WITH cte_WBBPCovEmpDishonesty(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.BP_CoverageEmployeeDishonestyId, 
	X.WB_BP_CoverageEmployeeDishonestyId,
	X.SessionId, 
	X.ERISAPlanName, 
	X.AuditConducted, 
	X.WhoPerformsAudit, 
	X.AuditRenderedTo, 
	X.BankAccountsReconciled, 
	X.CountersignatureRequired, 
	X.SecuritiesJointControl, 
	X.VacationRequired, 
	X.DesignatedAgentsAsEmployees
	FROM
	WB_BP_CoverageEmployeeDishonesty X
	inner join
	cte_WBBPCovEmpDishonesty Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_TRANS AS (
	SELECT
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
	SYSDATE AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_WB_BP_CoverageEmployeeDishonesty
),
WBBPCoverageEmployeeDishonestyStage AS (
	TRUNCATE TABLE WBBPCoverageEmployeeDishonestyStage;
	INSERT INTO WBBPCoverageEmployeeDishonestyStage
	(ExtractDate, SourceSystemid, BP_CoverageEmployeeDishonestyId, WB_BP_CoverageEmployeeDishonestyId, SessionId, ERISAPlanName, AuditConducted, WhoPerformsAudit, AuditRenderedTo, BankAccountsReconciled, CountersignatureRequired, SecuritiesJointControl, VacationRequired, DesignatedAgentsAsEmployees)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
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