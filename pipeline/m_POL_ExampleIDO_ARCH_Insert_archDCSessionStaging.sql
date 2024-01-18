WITH
SQ_DCSessionStaging AS (
	SELECT
		DCSessionStagingId,
		SessionId,
		ExampleQuoteId,
		UserName,
		CreateDateTime,
		Purpose,
		ExtractDate,
		SourceSystemId
	FROM DCSessionStaging
),
EXPTRANS AS (
	SELECT
	DCSessionStagingId,
	SessionId,
	ExampleQuoteId,
	UserName,
	CreateDateTime,
	Purpose,
	ExtractDate,
	SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM SQ_DCSessionStaging
),
archDCSessionStaging AS (
	INSERT INTO archDCSessionStaging
	(ExtractDate, SourceSystemId, AuditId, DCSessionStagingId, SessionId, ExampleQuoteId, UserName, CreateDateTime, Purpose)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
	DCSESSIONSTAGINGID, 
	SESSIONID, 
	EXAMPLEQUOTEID, 
	USERNAME, 
	CREATEDATETIME, 
	PURPOSE
	FROM EXPTRANS
),