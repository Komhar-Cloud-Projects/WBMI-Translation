WITH
SQ_WBCFCoverageOrdinanceOrLawStaging AS (
	SELECT
		ExtractDate,
		SourceSystemId,
		CF_CoverageOrdinanceOrLawId,
		WB_CF_CoverageOrdinanceOrLawId,
		SessionId,
		CoverageASelectDisplayString
	FROM WBCFCoverageOrdinanceOrLawStaging
),
EXP_Metadata AS (
	SELECT
	ExtractDate,
	SourceSystemId,
	CF_CoverageOrdinanceOrLawId,
	WB_CF_CoverageOrdinanceOrLawId,
	SessionId,
	CoverageASelectDisplayString,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM SQ_WBCFCoverageOrdinanceOrLawStaging
),
archWBCFCoverageOrdinanceOrLawStaging AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.archWBCFCoverageOrdinanceOrLawStaging
	(ExtractDate, SourceSystemId, AuditId, CF_CoverageOrdinanceOrLawId, WB_CF_CoverageOrdinanceOrLawId, SessionId, CoverageASelectDisplayString)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
	CF_COVERAGEORDINANCEORLAWID, 
	WB_CF_COVERAGEORDINANCEORLAWID, 
	SESSIONID, 
	COVERAGEASELECTDISPLAYSTRING
	FROM EXP_Metadata
),