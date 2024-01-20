WITH
SQ_WB_BP_CoverageEmploymentPracticesLiability AS (
	WITH cte_WBBPCoverageEmploymentPracticesLiability(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.CoverageId, 
	X.WB_BP_CoverageEmploymentPracticesLiabilityId, 
	X.SessionId, 
	X.NumberOfEmployees, 
	X.RetroactiveDate, 
	X.PriorLosses, 
	X.BillingLOB, 
	X.CommissionPlanID, 
	X.IsBillingSubline, 
	X.ParentBillingLOB, 
	X.PurePremium, 
	X.TransactionCommissionType, 
	X.TransactionCommissionValue,
	X.TransactionFinalCommissionValue 
	FROM
	WB_BP_CoverageEmploymentPracticesLiability X
	inner join
	cte_WBBPCoverageEmploymentPracticesLiability Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXPTRANS AS (
	SELECT
	SYSDATE AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
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
	FROM SQ_WB_BP_CoverageEmploymentPracticesLiability
),
WBBPCoverageEmploymentPracticesLiabilityStage AS (
	TRUNCATE TABLE WBBPCoverageEmploymentPracticesLiabilityStage;
	INSERT INTO WBBPCoverageEmploymentPracticesLiabilityStage
	(ExtractDate, SourceSystemId, CoverageId, WB_BP_CoverageEmploymentPracticesLiabilityId, SessionId, NumberOfEmployees, RetroactiveDate, PriorLosses, BillingLOB, CommissionPlanID, IsBillingSubline, ParentBillingLOB, PurePremium, TransactionCommissionType, TransactionCommissionValue, TransactionFinalCommissionValue)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
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
	FROM EXPTRANS
),