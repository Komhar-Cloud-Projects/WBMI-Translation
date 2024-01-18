WITH
SQ_DCDeductibleStaging AS (
	SELECT
		ObjectId,
		ObjectName,
		DeductibleId,
		SessionId,
		Type,
		Value,
		DataType,
		Scope,
		ExtractDate,
		SourceSystemId
	FROM DCDeductibleStaging
),
EXP_Metadata AS (
	SELECT
	ObjectId,
	ObjectName,
	DeductibleId,
	SessionId,
	Type,
	Value,
	DataType,
	Scope,
	ExtractDate,
	SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM SQ_DCDeductibleStaging
),
archDCDeductibleStaging AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.archDCDeductibleStaging
	(ObjectId, ObjectName, DeductibleId, SessionId, Type, Value, DataType, Scope, ExtractDate, SourceSystemId, AuditId)
	SELECT 
	OBJECTID, 
	OBJECTNAME, 
	DEDUCTIBLEID, 
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