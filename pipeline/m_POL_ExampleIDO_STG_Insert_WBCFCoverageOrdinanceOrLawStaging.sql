WITH
SQ_WB_CF_CoverageOrdinanceOrLaw AS (
	WITH cte_WBCFCoverageOrdinanceOrLaw(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.CF_CoverageOrdinanceOrLawId, 
	X.WB_CF_CoverageOrdinanceOrLawId, 
	X.SessionId, 
	X.CoverageASelectDisplayString 
	FROM
	WB_CF_CoverageOrdinanceOrLaw X
	inner join
	cte_WBCFCoverageOrdinanceOrLaw Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	CF_CoverageOrdinanceOrLawId,
	WB_CF_CoverageOrdinanceOrLawId,
	SessionId,
	CoverageASelectDisplayString,
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_WB_CF_CoverageOrdinanceOrLaw
),
WBCFCoverageOrdinanceOrLawStaging AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.WBCFCoverageOrdinanceOrLawStaging;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.WBCFCoverageOrdinanceOrLawStaging
	(ExtractDate, SourceSystemId, CF_CoverageOrdinanceOrLawId, WB_CF_CoverageOrdinanceOrLawId, SessionId, CoverageASelectDisplayString)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	CF_COVERAGEORDINANCEORLAWID, 
	WB_CF_COVERAGEORDINANCEORLAWID, 
	SESSIONID, 
	COVERAGEASELECTDISPLAYSTRING
	FROM EXP_Metadata
),