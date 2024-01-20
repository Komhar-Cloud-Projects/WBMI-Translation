WITH
SQ_WB_CL_CoverageExtortion AS (
	WITH cte_WBCLCoverageExtortion(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.CoverageId, 
	X.WB_CL_CoverageExtortionId, 
	X.SessionId, 
	X.BillingLOB, 
	X.CommissionPlanID, 
	X.IsBillingSubline, 
	X.ParentBillingLOB, 
	X.TransactionFinalCommissionValue
	FROM
	WB_CL_CoverageExtortion X
	inner join
	cte_WBCLCoverageExtortion Y on X.Sessionid = Y.Sessionid
	 @{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	SYSDATE AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	CoverageId,
	WB_CL_CoverageExtortionId,
	SessionId,
	BillingLOB,
	CommissionPlanID,
	IsBillingSubline,
	ParentBillingLOB,
	TransactionFinalCommissionValue,
	SessionId1,
	CreateDateTime
	FROM SQ_WB_CL_CoverageExtortion
),
WBCLCoverageExtortionStage AS (
	TRUNCATE TABLE WBCLCoverageExtortionStage;
	INSERT INTO WBCLCoverageExtortionStage
	(ExtractDate, SourceSystemid, CoverageId, WB_CL_CoverageExtortionId, SessionId, BillingLOB, CommissionPlanID, IsBillingSubline, ParentBillingLOB, TransactionFinalCommissionValue)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	COVERAGEID, 
	WB_CL_COVERAGEEXTORTIONID, 
	SESSIONID, 
	BILLINGLOB, 
	COMMISSIONPLANID, 
	ISBILLINGSUBLINE, 
	PARENTBILLINGLOB, 
	TRANSACTIONFINALCOMMISSIONVALUE
	FROM EXP_Metadata
),