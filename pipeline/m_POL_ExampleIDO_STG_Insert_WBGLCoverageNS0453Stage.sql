WITH
SQ_WB_GL_CoverageNS0453 AS (
	WITH cte_WBGLLocationAccount(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.CoverageId,
	X.WB_GL_CoverageNS0453Id,
	X.SessionId,
	X.RadonRetroactiveDate,
	X.LimitedPollutionRetroDate 
	FROM
	 WB_GL_CoverageNS0453 X
	inner join
	cte_WBGLLocationAccount Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata1 AS (
	SELECT
	SYSDATE AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	CoverageId,
	WB_GL_CoverageNS0453Id,
	SessionId,
	RadonRetroactiveDate,
	LimitedPollutionRetroDate
	FROM SQ_WB_GL_CoverageNS0453
),
WBGLCoverageNS0453Stage AS (
	TRUNCATE TABLE WBGLCoverageNS0453Stage;
	INSERT INTO WBGLCoverageNS0453Stage
	(ExtractDate, SourceSystemId, CoverageId, WBGLCoverageNS0453Id, SessionId, RadonRetroactiveDate, LimitedPollutionRetroDate)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	COVERAGEID, 
	WB_GL_CoverageNS0453Id AS WBGLCOVERAGENS0453ID, 
	SESSIONID, 
	RADONRETROACTIVEDATE, 
	LIMITEDPOLLUTIONRETRODATE
	FROM EXP_Metadata1
),