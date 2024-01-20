WITH
SQ_DCTaxSurchargeStaging AS (
	SELECT
		ObjectId,
		ObjectName,
		TaxSurchargeId,
		SessionId,
		Id,
		Type,
		Scope,
		Amount,
		Change,
		Written,
		ExtractDate,
		SourceSystemId,
		Rate
	FROM DCTaxSurchargeStaging
),
EXP_Metadata AS (
	SELECT
	ObjectId,
	ObjectName,
	TaxSurchargeId,
	SessionId,
	Id,
	Type,
	Scope,
	Amount,
	Change,
	Written,
	ExtractDate,
	SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	Rate
	FROM SQ_DCTaxSurchargeStaging
),
archDCTaxSurchargeStaging AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.archDCTaxSurchargeStaging
	(ObjectId, ObjectName, TaxSurchargeId, SessionId, Id, Type, Scope, Amount, Change, Written, ExtractDate, SourceSystemId, AuditId, Rate)
	SELECT 
	OBJECTID, 
	OBJECTNAME, 
	TAXSURCHARGEID, 
	SESSIONID, 
	ID, 
	TYPE, 
	SCOPE, 
	AMOUNT, 
	CHANGE, 
	WRITTEN, 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
	RATE
	FROM EXP_Metadata
),