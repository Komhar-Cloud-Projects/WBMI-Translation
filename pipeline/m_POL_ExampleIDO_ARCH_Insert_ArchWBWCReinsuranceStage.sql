WITH
SQ_WBWCReinsuranceStage AS (
	SELECT
		WBWCReinsuranceStageId,
		ExtractDate,
		SourceSystemId,
		WBCLReinsuranceId,
		WBWCReinsuranceId,
		SessionId,
		CertificateReceived,
		Premium,
		PolicyTerms,
		RetentionLimit,
		CededLimit
	FROM WBWCReinsuranceStage
),
EXP_Metadata AS (
	SELECT
	WBWCReinsuranceStageId,
	ExtractDate,
	SourceSystemId,
	WBCLReinsuranceId,
	WBWCReinsuranceId,
	SessionId,
	CertificateReceived,
	Premium,
	PolicyTerms,
	RetentionLimit,
	CededLimit,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditId
	FROM SQ_WBWCReinsuranceStage
),
ArchWBWCReinsuranceStage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ArchWBWCReinsuranceStage
	(ExtractDate, SourceSystemId, AuditId, WBCLReinsuranceId, WBWCReinsuranceId, SessionId, CertificateReceived, Premium, PolicyTerms, RetentionLimit, CededLimit)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	AUDITID, 
	WBCLREINSURANCEID, 
	WBWCREINSURANCEID, 
	SESSIONID, 
	CERTIFICATERECEIVED, 
	PREMIUM, 
	POLICYTERMS, 
	RETENTIONLIMIT, 
	CEDEDLIMIT
	FROM EXP_Metadata
),