WITH
SQ_WB_CL_CoverageNetworkSecurity AS (
	WITH cte_WBCLCoverageNetworkSecurity(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.CoverageId, 
	X.WB_CL_CoverageNetworkSecurityId, 
	X.SessionId, 
	X.Selected,
	X.ThirdPartyBusiness, 
	X.BillingLOB, 
	X.CommissionPlanID, 
	X.IsBillingSubline, 
	X.ParentBillingLOB, 
	X.TransactionFinalCommissionValue
	FROM
	WB_CL_CoverageNetworkSecurity X
	inner join
	cte_WBCLCoverageNetworkSecurity Y on X.Sessionid = Y.Sessionid
	 @{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	SYSDATE AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	SessionId1,
	CreateDateTime,
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
	FROM SQ_WB_CL_CoverageNetworkSecurity
),
WBCLCoverageNetworkSecurityStage AS (
	TRUNCATE TABLE WBCLCoverageNetworkSecurityStage;
	INSERT INTO WBCLCoverageNetworkSecurityStage
	(ExtractDate, SourceSystemid, CoverageId, WB_CL_CoverageNetworkSecurityId, SessionId, Selected, ThirdPartyBusiness, BillingLOB, CommissionPlanID, IsBillingSubline, ParentBillingLOB, TransactionFinalCommissionValue)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
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