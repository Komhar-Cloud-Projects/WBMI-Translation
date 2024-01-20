WITH
SQ_DCContactStaging AS (
	SELECT
		DCContactStagingId,
		PartyId,
		ContactId,
		SessionId,
		Type,
		PhoneNumber,
		PhoneExtension,
		Email,
		ExtractDate,
		SourceSystemId
	FROM DCContactStaging
),
EXP_Metadata AS (
	SELECT
	DCContactStagingId,
	PartyId,
	ContactId,
	SessionId,
	Type,
	PhoneNumber,
	PhoneExtension,
	Email,
	ExtractDate,
	SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM SQ_DCContactStaging
),
archDCContactStaging AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.archDCContactStaging
	(ExtractDate, SourceSystemId, AuditId, DCContactStagingId, PartyId, ContactId, SessionId, Type, PhoneNumber, PhoneExtension, Email)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
	DCCONTACTSTAGINGID, 
	PARTYID, 
	CONTACTID, 
	SESSIONID, 
	TYPE, 
	PHONENUMBER, 
	PHONEEXTENSION, 
	EMAIL
	FROM EXP_Metadata
),