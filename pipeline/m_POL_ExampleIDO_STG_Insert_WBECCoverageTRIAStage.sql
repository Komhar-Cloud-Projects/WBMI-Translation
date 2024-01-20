WITH
SQ_WB_EC_CoverageTRIA AS (
	WITH cte_WB_EC_CoverageTRIA(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.CoverageId,
	X.WB_EC_CoverageTRIAId,
	X.SessionId 
	FROM
	WB_EC_CoverageTRIA X
	inner join
	cte_WB_EC_CoverageTRIA Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	SYSDATE AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	CoverageId,
	WB_EC_CoverageTRIAId,
	SessionId
	FROM SQ_WB_EC_CoverageTRIA
),
WBECCoverageTRIAStage AS (
	TRUNCATE TABLE WBECCoverageTRIAStage;
	INSERT INTO WBECCoverageTRIAStage
	(ExtractDate, SourceSystemId, CoverageId, WB_EC_CoverageTRIAId, SessionId)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	COVERAGEID, 
	WB_EC_COVERAGETRIAID, 
	SESSIONID
	FROM EXP_Metadata
),