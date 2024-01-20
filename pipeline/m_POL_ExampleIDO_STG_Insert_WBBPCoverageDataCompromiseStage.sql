WITH
SQ_WB_BP_CoverageDataCompromise AS (
	WITH cte_WBBPCoverageDataCompromise(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.CoverageId, 
	X.WB_BP_CoverageDataCompromiseId, 
	X.SessionId, 
	X.ResponseExpensesTotalPremium, 
	X.DefenseAndLiabilityTotalPremium, 
	X.EachSuitDataCompromiseStaticText, 
	X.WB_CL_CoverageDataCompromiseId, 
	X.BillingLOB, 
	X.CommissionPlanID, 
	X.IsBillingSubline, 
	X.ParentBillingLOB, 
	X.PurePremium, 
	X.TransactionCommissionType, 
	X.TransactionCommissionValue, 
	X.TransactionFinalCommissionValue 
	FROM
	WB_BP_CoverageDataCompromise X
	inner join
	cte_WBBPCoverageDataCompromise Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	SYSDATE AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	CoverageId,
	WB_BP_CoverageDataCompromiseId,
	SessionId,
	ResponseExpensesTotalPremium,
	DefenseAndLiabilityTotalPremium,
	EachSuitDataCompromiseStaticText,
	WB_CL_CoverageDataCompromiseId,
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
	FROM SQ_WB_BP_CoverageDataCompromise
),
WBBPCoverageDataCompromiseStage AS (
	TRUNCATE TABLE WBBPCoverageDataCompromiseStage;
	INSERT INTO WBBPCoverageDataCompromiseStage
	(ExtractDate, SourceSystemId, CoverageId, WB_BP_CoverageDataCompromiseId, SessionId, ResponseExpensesTotalPremium, DefenseAndLiabilityTotalPremium, EachSuitDataCompromiseStaticText, BillingLOB, CommissionPlanID, IsBillingSubline, ParentBillingLOB, PurePremium, TransactionCommissionType, TransactionCommissionValue, TransactionFinalCommissionValue, WB_CL_CoverageDataCompromiseId)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
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