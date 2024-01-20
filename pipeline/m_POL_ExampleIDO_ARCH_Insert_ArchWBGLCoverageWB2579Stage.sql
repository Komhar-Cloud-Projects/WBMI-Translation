WITH
SQ_WBGLCoverageWB2579Stage AS (
	SELECT
		WBGLCoverageWB2579StageId,
		ExtractDate,
		SourceSystemId,
		CoverageId,
		WB_GL_CoverageWB2579Id,
		SessionId,
		RetroactiveDate
	FROM WBGLCoverageWB2579Stage
),
EXP_Metadata AS (
	SELECT
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	WBGLCoverageWB2579StageId,
	ExtractDate,
	SourceSystemId,
	CoverageId,
	WB_GL_CoverageWB2579Id,
	SessionId,
	RetroactiveDate
	FROM SQ_WBGLCoverageWB2579Stage
),
ArchWBGLCoverageWB2579Stage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ArchWBGLCoverageWB2579Stage
	(ExtractDate, SourceSystemId, AuditId, WBGLCoverageWB2579StageId, CoverageId, WB_GL_CoverageWB2579Id, SessionId, RetroactiveDate)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
	WBGLCOVERAGEWB2579STAGEID, 
	COVERAGEID, 
	WB_GL_COVERAGEWB2579ID, 
	SESSIONID, 
	RETROACTIVEDATE
	FROM EXP_Metadata
),