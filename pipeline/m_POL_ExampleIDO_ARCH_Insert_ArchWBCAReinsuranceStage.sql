WITH
SQ_WBCAReinsuranceStage AS (
	SELECT
		WBCAReinsuranceStageId,
		ExtractDate,
		SourceSystemId,
		WBCLReinsuranceId,
		WBCAReinsuranceId,
		SessionId,
		PurchasedEachAccidentLimit,
		AddedCaption
	FROM WBCAReinsuranceStage
),
EXP_Metadata AS (
	SELECT
	WBCAReinsuranceStageId,
	ExtractDate,
	SourceSystemId,
	WBCLReinsuranceId,
	WBCAReinsuranceId,
	SessionId,
	PurchasedEachAccidentLimit,
	AddedCaption,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM SQ_WBCAReinsuranceStage
),
ArchWBCAReinsuranceStage AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ArchWBCAReinsuranceStage
	(ExtractDate, SourceSystemId, AuditId, WBCAReinsuranceStageId, WBCLReinsuranceId, WBCAReinsuranceId, SessionId, PurchasedEachAccidentLimit, AddedCaption)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
	WBCAREINSURANCESTAGEID, 
	WBCLREINSURANCEID, 
	WBCAREINSURANCEID, 
	SESSIONID, 
	PURCHASEDEACHACCIDENTLIMIT, 
	ADDEDCAPTION
	FROM EXP_Metadata
),