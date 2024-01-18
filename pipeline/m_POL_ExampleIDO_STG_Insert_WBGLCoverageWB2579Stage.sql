WITH
SQ_WB_GL_CoverageWB2579 AS (
	WITH cte_WBGLCoverageWB2579(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.CoverageId, 
	X.WB_GL_CoverageWB2579Id, 
	X.SessionId, 
	X.RetroactiveDate 
	FROM
	WB_GL_CoverageWB2579 X
	inner join
	cte_WBGLCoverageWB2579 Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
	SELECT
	SYSDATE AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	CoverageId,
	WB_GL_CoverageWB2579Id,
	SessionId,
	RetroactiveDate
	FROM SQ_WB_GL_CoverageWB2579
),
WBGLCoverageWB2579Stage AS (
	TRUNCATE TABLE WBGLCoverageWB2579Stage;
	INSERT INTO WBGLCoverageWB2579Stage
	(ExtractDate, SourceSystemId, CoverageId, WB_GL_CoverageWB2579Id, SessionId, RetroactiveDate)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	COVERAGEID, 
	WB_GL_COVERAGEWB2579ID, 
	SESSIONID, 
	RETROACTIVEDATE
	FROM EXP_Metadata
),