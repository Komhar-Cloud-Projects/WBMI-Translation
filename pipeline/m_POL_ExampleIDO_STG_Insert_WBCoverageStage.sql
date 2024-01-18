WITH
SQ_WB_Coverage AS (
	WITH cte_WBCoverage(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.CoverageId, 
	X.WB_CoverageId, 
	X.SessionId, 
	X.Indicator, 
	X.IndicatorbValue 
	FROM
	WB_Coverage X
	inner join
	cte_WBCoverage Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	CoverageId,
	WB_CoverageId,
	SessionId,
	Indicator AS i_Indicator,
	IndicatorbValue AS i_IndicatorbValue,
	-- *INF*: DECODE(i_Indicator,'T','1','F','0',NULL)
	DECODE(
	    i_Indicator,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_Indicator,
	-- *INF*: DECODE(i_IndicatorbValue,'T','1','F','0',NULL)
	DECODE(
	    i_IndicatorbValue,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_IndicatorbValue,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_WB_Coverage
),
WBCoverageStage AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.WBCoverageStage;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.WBCoverageStage
	(ExtractDate, SourceSystemId, CoverageId, WBCoverageId, SessionId, Indicator, IndicatorbValue)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	COVERAGEID, 
	WB_CoverageId AS WBCOVERAGEID, 
	SESSIONID, 
	o_Indicator AS INDICATOR, 
	o_IndicatorbValue AS INDICATORBVALUE
	FROM EXP_Metadata
),