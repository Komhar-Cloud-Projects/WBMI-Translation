WITH
SQ_WBGLCoverageNS0432Stage AS (
	SELECT
		WBGLCoverageNS0432StageId,
		ExtractDate,
		SourceSystemid,
		CoverageId,
		WB_GL_CoverageNS0432Id,
		SessionId,
		BodilyInjuryAndPropertyDamageLimitedCoverage,
		PersonalAndAdvertisingInjuryLimitedCoverage
	FROM WBGLCoverageNS0432Stage
),
EXP_MetaData AS (
	SELECT
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	WBGLCoverageNS0432StageId,
	ExtractDate,
	SourceSystemid,
	CoverageId,
	WB_GL_CoverageNS0432Id,
	SessionId,
	BodilyInjuryAndPropertyDamageLimitedCoverage,
	PersonalAndAdvertisingInjuryLimitedCoverage
	FROM SQ_WBGLCoverageNS0432Stage
),
ArchWBGLCoverageNS0432Stage AS (
	INSERT INTO ArchWBGLCoverageNS0432Stage
	(ExtractDate, SourceSystemId, AuditId, WBGLCoverageNS0432StageId, CoverageId, WB_GL_CoverageNS0432Id, SessionId, BodilyInjuryAndPropertyDamageLimitedCoverage, PersonalAndAdvertisingInjuryLimitedCoverage)
	SELECT 
	EXTRACTDATE, 
	SourceSystemid AS SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
	WBGLCOVERAGENS0432STAGEID, 
	COVERAGEID, 
	WB_GL_COVERAGENS0432ID, 
	SESSIONID, 
	BODILYINJURYANDPROPERTYDAMAGELIMITEDCOVERAGE, 
	PERSONALANDADVERTISINGINJURYLIMITEDCOVERAGE
	FROM EXP_MetaData
),