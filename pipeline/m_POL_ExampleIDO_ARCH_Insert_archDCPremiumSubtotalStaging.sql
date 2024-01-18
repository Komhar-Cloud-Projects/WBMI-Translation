WITH
SQ_DCPremiumSubtotalStaging AS (
	SELECT
		ObjectId,
		ObjectName,
		PremiumSubtotalId,
		SessionId,
		Type,
		Value,
		Change,
		Written,
		Prior,
		ExtractDate,
		SourceSystemId
	FROM DCPremiumSubtotalStaging
),
EXP_Metadata AS (
	SELECT
	ObjectId,
	ObjectName,
	PremiumSubtotalId,
	SessionId,
	Type,
	Value,
	Change,
	Written,
	Prior,
	ExtractDate,
	SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM SQ_DCPremiumSubtotalStaging
),
archDCPremiumSubtotalStaging AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.archDCPremiumSubtotalStaging
	(ObjectId, ObjectName, PremiumSubtotalId, SessionId, Type, Value, Change, Written, Prior, ExtractDate, SourceSystemId, AuditId)
	SELECT 
	OBJECTID, 
	OBJECTNAME, 
	PREMIUMSUBTOTALID, 
	SESSIONID, 
	TYPE, 
	VALUE, 
	CHANGE, 
	WRITTEN, 
	PRIOR, 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID
	FROM EXP_Metadata
),