WITH
SQ_DCStatCodeStaging AS (
	SELECT
		ObjectId,
		ObjectName,
		StatCodeId,
		SessionId,
		Type,
		Value,
		Scope,
		ExtractDate,
		SourceSystemId
	FROM DCStatCodeStaging
),
EXP_Metadata AS (
	SELECT
	ObjectId,
	ObjectName,
	StatCodeId,
	SessionId,
	Type,
	Value,
	Scope,
	ExtractDate,
	SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM SQ_DCStatCodeStaging
),
archDCStatCodeStaging AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.archDCStatCodeStaging
	(ObjectId, ObjectName, StatCodeId, SessionId, Type, Value, Scope, ExtractDate, SourceSystemId, AuditId)
	SELECT 
	OBJECTID, 
	OBJECTNAME, 
	STATCODEID, 
	SESSIONID, 
	TYPE, 
	VALUE, 
	SCOPE, 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID
	FROM EXP_Metadata
),