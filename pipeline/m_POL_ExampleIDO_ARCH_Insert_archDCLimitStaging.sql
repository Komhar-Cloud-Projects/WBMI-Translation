WITH
SQ_DCLimitStaging AS (
	SELECT
		ObjectId,
		ObjectName,
		LimitId,
		SessionId,
		Type,
		Value,
		DataType,
		Scope,
		ExtractDate,
		SourceSystemId
	FROM DCLimitStaging
),
EXP_Metadata AS (
	SELECT
	ObjectId,
	ObjectName,
	LimitId,
	SessionId,
	Type,
	Value,
	DataType,
	Scope,
	ExtractDate,
	SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM SQ_DCLimitStaging
),
archDCLimitStaging AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.archDCLimitStaging
	(ObjectId, ObjectName, LimitId, SessionId, Type, Value, DataType, Scope, ExtractDate, SourceSystemId, AuditId)
	SELECT 
	OBJECTID, 
	OBJECTNAME, 
	LIMITID, 
	SESSIONID, 
	TYPE, 
	VALUE, 
	DATATYPE, 
	SCOPE, 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID
	FROM EXP_Metadata
),