WITH
SQ_DC_BP_CoverageBusinessIncomeOrdinaryPayroll AS (
	WITH cte_DCBPCoverageBusinessIncomeOrdinaryPayroll(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.CoverageId, 
	X.BP_CoverageBusinessIncomeOrdinaryPayrollId, 
	X.SessionId, 
	X.Days 
	FROM
	DC_BP_CoverageBusinessIncomeOrdinaryPayroll X
	inner join
	cte_DCBPCoverageBusinessIncomeOrdinaryPayroll Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	CoverageId,
	BP_CoverageBusinessIncomeOrdinaryPayrollId,
	SessionId,
	Days,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_DC_BP_CoverageBusinessIncomeOrdinaryPayroll
),
DCBPCoverageBusinessIncomeOrdinaryPayrollStage AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.DCBPCoverageBusinessIncomeOrdinaryPayrollStage;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.DCBPCoverageBusinessIncomeOrdinaryPayrollStage
	(ExtractDate, SourceSystemId, CoverageId, BP_CoverageBusinessIncomeOrdinaryPayrollId, SessionId, Days)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	COVERAGEID, 
	BP_COVERAGEBUSINESSINCOMEORDINARYPAYROLLID, 
	SESSIONID, 
	DAYS
	FROM EXP_Metadata
),