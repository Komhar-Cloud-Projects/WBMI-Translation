WITH
SQ_WBReasonStaging AS (
	SELECT
		TransactionId,
		WB_ReasonId,
		SessionId,
		Code,
		CodeCaption,
		Description,
		ExtractDate,
		SourceSystemId
	FROM WBReasonStaging
),
EXP_Metadata AS (
	SELECT
	TransactionId,
	WB_ReasonId,
	SessionId,
	Code,
	CodeCaption,
	Description,
	ExtractDate,
	SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM SQ_WBReasonStaging
),
archWBReasonStaging AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.archWBReasonStaging
	(TransactionId, WB_ReasonId, SessionId, Code, CodeCaption, Description, ExtractDate, SourceSystemId, AuditId)
	SELECT 
	TRANSACTIONID, 
	WB_REASONID, 
	SESSIONID, 
	CODE, 
	CODECAPTION, 
	DESCRIPTION, 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID
	FROM EXP_Metadata
),