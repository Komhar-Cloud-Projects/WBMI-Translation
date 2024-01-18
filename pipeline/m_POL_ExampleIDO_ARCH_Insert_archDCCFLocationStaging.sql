WITH
SQ_DCCFLocationStaging AS (
	SELECT
		CF_LocationId,
		SessionId,
		Id,
		Description,
		Number,
		ExtractDate,
		SourceSystemId
	FROM DCCFLocationStaging
),
EXP_Metadata AS (
	SELECT
	CF_LocationId,
	SessionId,
	Id,
	Description,
	Number,
	ExtractDate,
	SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM SQ_DCCFLocationStaging
),
archDCCFLocationStaging AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.archDCCFLocationStaging
	(CF_LocationId, SessionId, Id, Description, Number, ExtractDate, SourceSystemId, AuditId)
	SELECT 
	CF_LOCATIONID, 
	SESSIONID, 
	ID, 
	DESCRIPTION, 
	NUMBER, 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID
	FROM EXP_Metadata
),