WITH
SQ_WBGLCoverageNS0453Stage AS (
	SELECT
		WBGLCoverageNS0453StageId,
		ExtractDate,
		SourceSystemId,
		CoverageId,
		WBGLCoverageNS0453Id AS WB_GL_CoverageNS0453Id,
		SessionId,
		RadonRetroactiveDate,
		LimitedPollutionRetroDate
	FROM WBGLCoverageNS0453Stage
),
EXP_Metadata1 AS (
	SELECT
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditId,
	WBGLCoverageNS0453StageId,
	ExtractDate,
	SourceSystemId,
	CoverageId,
	WB_GL_CoverageNS0453Id,
	SessionId,
	RadonRetroactiveDate,
	LimitedPollutionRetroDate
	FROM SQ_WBGLCoverageNS0453Stage
),
ArchWBGLCoverageNS0453Stage AS (
	INSERT INTO ArchWBGLCoverageNS0453Stage
	(ExtractDate, AuditId, WBGLCoverageNS0453StageId, SourceSystemId, CoverageId, WBGLCoverageNS0453Id, SessionId, RadonRetroactiveDate, LimitedPollutionRetroDate)
	SELECT 
	EXTRACTDATE, 
	AUDITID, 
	WBGLCOVERAGENS0453STAGEID, 
	SOURCESYSTEMID, 
	COVERAGEID, 
	WB_GL_CoverageNS0453Id AS WBGLCOVERAGENS0453ID, 
	SESSIONID, 
	RADONRETROACTIVEDATE, 
	LIMITEDPOLLUTIONRETRODATE
	FROM EXP_Metadata1
),