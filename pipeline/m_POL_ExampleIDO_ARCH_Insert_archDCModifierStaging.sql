WITH
SQ_DCModifierStaging AS (
	SELECT
		DCModifierStagingId,
		ObjectId,
		ObjectName,
		ModifierId,
		SessionId,
		Type,
		Value,
		Scope,
		ExtractDate,
		SourceSystemId
	FROM DCModifierStaging
),
EXP_Metadata AS (
	SELECT
	DCModifierStagingId,
	ObjectId,
	ObjectName,
	ModifierId,
	SessionId,
	Type,
	Value,
	Scope,
	ExtractDate,
	SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM SQ_DCModifierStaging
),
archDCModifierStaging AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.archDCModifierStaging
	(ExtractDate, SourceSystemId, AuditId, DCModifierStagingId, ObjectId, ObjectName, ModifierId, SessionId, Type, Value, Scope)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
	DCMODIFIERSTAGINGID, 
	OBJECTID, 
	OBJECTNAME, 
	MODIFIERID, 
	SESSIONID, 
	TYPE, 
	VALUE, 
	SCOPE
	FROM EXP_Metadata
),