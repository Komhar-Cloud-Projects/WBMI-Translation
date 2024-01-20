WITH
SQ_WB_GL_CoverageWB2525 AS (
	WITH cte_WBGLCoverageWB2525(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.CoverageId, 
	X.WB_GL_CoverageWB2525Id, 
	X.SessionId, 
	X.BillingLOB, 
	X.CommissionPlanId, 
	X.IsBillingSubline, 
	X.ParentBillingLOB, 
	X.PurePremium, 
	X.TransactionCommissionType, 
	X.TransactionCommissionValue, 
	X.TransactionFinalCommissionValue 
	FROM
	WB_GL_CoverageWB2525 X
	inner join
	cte_WBGLCoverageWB2525 Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
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
	FROM SQ_WB_GL_CoverageWB2525
),
WBGLCoverageWB2525Stage AS (
	TRUNCATE TABLE WBGLCoverageWB2525Stage;
	INSERT INTO WBGLCoverageWB2525Stage
	(ExtractDate, SourceSystemId, CoverageId, WB_GL_CoverageWB2525Id, SessionId, BillingLOB, CommissionPlanId, IsBillingSubline, ParentBillingLOB, PurePremium, TransactionCommissionType, TransactionCommissionValue, TransactionFinalCommissionValue)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
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