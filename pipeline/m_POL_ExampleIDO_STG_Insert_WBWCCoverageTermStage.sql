WITH
SQ_WB_WC_CoverageTerm AS (
	WITH cte_WBWCCoverageTerm(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.CoverageId, 
	X.WB_CoverageId, 
	X.WB_WC_CoverageTermId, 
	X.SessionId, 
	X.PeriodStartDate, 
	X.PeriodEndDate,
	X.TermRateEffectiveDate,
	X.TermType
	FROM  
	WB_WC_CoverageTerm X
	inner join
	cte_WBWCCoverageTerm Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	CoverageId,
	WB_CoverageId,
	WB_WC_CoverageTermId,
	SessionId,
	PeriodStartDate,
	PeriodEndDate,
	TermRateEffectiveDate,
	TermType,
	SYSDATE AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_WB_WC_CoverageTerm
),
WBWCCoverageTermStage AS (
	TRUNCATE TABLE WBWCCoverageTermStage;
	INSERT INTO WBWCCoverageTermStage
	(ExtractDate, SourceSystemId, CoverageId, WB_CoverageId, WB_WC_CoverageTermId, SessionId, PeriodStartDate, PeriodEndDate, TermRateEffectivedate, TermType)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	COVERAGEID, 
	WB_COVERAGEID, 
	WB_WC_COVERAGETERMID, 
	SESSIONID, 
	PERIODSTARTDATE, 
	PERIODENDDATE, 
	TermRateEffectiveDate AS TERMRATEEFFECTIVEDATE, 
	TERMTYPE
	FROM EXP_Metadata
),