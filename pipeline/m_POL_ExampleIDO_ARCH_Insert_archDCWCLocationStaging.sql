WITH
SQ_DCWCLocationStaging AS (
	SELECT
		WC_LocationId,
		SessionId,
		Id,
		Description,
		NumberOfEmployees,
		ExtractDate,
		SourceSystemId,
		WC_StateXmlId,
		Number
	FROM DCWCLocationStaging
),
EXP_Metadata AS (
	SELECT
	WC_LocationId,
	SessionId,
	Id,
	Description,
	NumberOfEmployees,
	ExtractDate,
	SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	WC_StateXmlId,
	Number
	FROM SQ_DCWCLocationStaging
),
archDCWCLocationStaging AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.archDCWCLocationStaging
	(WC_LocationId, SessionId, Id, Description, NumberOfEmployees, ExtractDate, SourceSystemId, AuditId, WC_StateXmlId, Number)
	SELECT 
	WC_LOCATIONID, 
	SESSIONID, 
	ID, 
	DESCRIPTION, 
	NUMBEROFEMPLOYEES, 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
	WC_STATEXMLID, 
	NUMBER
	FROM EXP_Metadata
),