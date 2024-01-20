WITH
SQ_DC_BP_CoverageBusinessIncomeExtendedPeriod AS (
	WITH cte_DCBPCoverageBusinessIncomeExtendedPeriod(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.CoverageId, 
	X.BP_CoverageBusinessIncomeExtendedPeriodId, 
	X.SessionId, 
	X.Days 
	FROM
	 DC_BP_CoverageBusinessIncomeExtendedPeriod X
	inner join
	cte_DCBPCoverageBusinessIncomeExtendedPeriod Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	CoverageId,
	BP_CoverageBusinessIncomeExtendedPeriodId,
	SessionId,
	Days,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_DC_BP_CoverageBusinessIncomeExtendedPeriod
),
DCBPCoverageBusinessIncomeExtendedPeriodStage AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.DCBPCoverageBusinessIncomeExtendedPeriodStage;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.DCBPCoverageBusinessIncomeExtendedPeriodStage
	(ExtractDate, SourceSystemId, CoverageId, BP_CoverageBusinessIncomeExtendedPeriodId, SessionId, Days)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	COVERAGEID, 
	BP_COVERAGEBUSINESSINCOMEEXTENDEDPERIODID, 
	SESSIONID, 
	DAYS
	FROM EXP_Metadata
),