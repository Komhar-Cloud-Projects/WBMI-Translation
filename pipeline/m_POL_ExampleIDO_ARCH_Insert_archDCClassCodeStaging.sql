WITH
SQ_DCClassCodeStaging AS (
	SELECT
		ObjectId,
		ObjectName,
		ClassCodeId,
		SessionId,
		Type,
		Value,
		ExtractDate,
		SourceSystemId
	FROM DCClassCodeStaging
),
EXP_Metadata AS (
	SELECT
	ObjectId,
	ObjectName,
	ClassCodeId,
	SessionId,
	Type,
	Value,
	ExtractDate,
	SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM SQ_DCClassCodeStaging
),
archDCClassCodeStaging AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.archDCClassCodeStaging
	(ObjectId, ObjectName, ClassCodeId, SessionId, Type, Value, ExtractDate, SourceSystemId, AuditId)
	SELECT 
	OBJECTID, 
	OBJECTNAME, 
	CLASSCODEID, 
	SESSIONID, 
	TYPE, 
	VALUE, 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID
	FROM EXP_Metadata
),