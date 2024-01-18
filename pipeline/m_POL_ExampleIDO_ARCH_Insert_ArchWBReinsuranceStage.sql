WITH
SQ_WBReinsuranceStage AS (
	SELECT
		WBReinsuranceStageId,
		ExtractDate,
		SourceSystemId,
		ReinsuranceId,
		WBReinsuranceId,
		SessionId,
		CertificateReceived,
		GrossReinsurancePremium,
		NetReinsurancePremium
	FROM WBReinsuranceStage
),
EXP_Metadata AS (
	SELECT
	WBReinsuranceStageId,
	ExtractDate,
	SourceSystemId,
	ReinsuranceId,
	WBReinsuranceId,
	SessionId,
	CertificateReceived,
	GrossReinsurancePremium,
	NetReinsurancePremium,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditId
	FROM SQ_WBReinsuranceStage
),
ArchWBReinsuranceStage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ArchWBReinsuranceStage
	(ExtractDate, SourceSystemId, AuditId, ReinsuranceId, WBReinsuranceId, SessionId, CertificateReceived, GrossReinsurancePremium, NetReinsurancePremium)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	AUDITID, 
	REINSURANCEID, 
	WBREINSURANCEID, 
	SESSIONID, 
	CERTIFICATERECEIVED, 
	GROSSREINSURANCEPREMIUM, 
	NETREINSURANCEPREMIUM
	FROM EXP_Metadata
),