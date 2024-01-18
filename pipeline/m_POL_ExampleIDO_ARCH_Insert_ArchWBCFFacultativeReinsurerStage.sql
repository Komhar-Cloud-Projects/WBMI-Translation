WITH
SQ_WBCFFacultativeReinsurerStage AS (
	SELECT
		WBCFFacultativeReinsurerStageId,
		ExtractDate,
		SourceSystemId,
		WBCFReinsuranceId,
		WBCFFacultativeReinsurerId,
		SessionId,
		CertificateReceived,
		ReinsurerName,
		Type,
		AmountCeded,
		ReinsurerPremium
	FROM WBCFFacultativeReinsurerStage
),
EXP_Metadata AS (
	SELECT
	WBCFFacultativeReinsurerStageId,
	ExtractDate,
	SourceSystemId,
	WBCFReinsuranceId,
	WBCFFacultativeReinsurerId,
	SessionId,
	CertificateReceived,
	ReinsurerName,
	Type,
	AmountCeded,
	ReinsurerPremium,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditId
	FROM SQ_WBCFFacultativeReinsurerStage
),
ArchWBCFFacultativeReinsurerStage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ArchWBCFFacultativeReinsurerStage
	(ExtractDate, SourceSystemId, AuditId, WBCFFacultativeReinsurerStageId, WBCFReinsuranceId, WBCFFacultativeReinsurerId, SessionId, CertificateReceived, ReinsurerName, Type, AmountCeded, ReinsurerPremium)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	AUDITID, 
	WBCFFACULTATIVEREINSURERSTAGEID, 
	WBCFREINSURANCEID, 
	WBCFFACULTATIVEREINSURERID, 
	SESSIONID, 
	CERTIFICATERECEIVED, 
	REINSURERNAME, 
	TYPE, 
	AMOUNTCEDED, 
	REINSURERPREMIUM
	FROM EXP_Metadata
),