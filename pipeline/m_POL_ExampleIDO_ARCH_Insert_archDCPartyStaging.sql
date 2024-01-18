WITH
SQ_DCPartyStaging AS (
	SELECT
		PartyId,
		SessionId,
		PartyXmlId,
		Type,
		OtherType,
		Name,
		DateOfBirth,
		Gender,
		FirstName,
		LastName,
		MiddleName,
		MaritalStatus,
		Title,
		Reference,
		ContactName,
		ExtractDate,
		SourceSystemId
	FROM DCPartyStaging
),
EXP_Metadata AS (
	SELECT
	PartyId,
	SessionId,
	PartyXmlId,
	Type,
	OtherType,
	Name,
	DateOfBirth,
	Gender,
	FirstName,
	LastName,
	MiddleName,
	MaritalStatus,
	Title,
	Reference,
	ContactName,
	ExtractDate,
	SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM SQ_DCPartyStaging
),
archDCPartyStaging AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.archDCPartyStaging
	(PartyId, SessionId, PartyXmlId, Type, OtherType, Name, DateOfBirth, Gender, FirstName, LastName, MiddleName, MaritalStatus, Title, Reference, ContactName, ExtractDate, SourceSystemId, AuditId)
	SELECT 
	PARTYID, 
	SESSIONID, 
	PARTYXMLID, 
	TYPE, 
	OTHERTYPE, 
	NAME, 
	DATEOFBIRTH, 
	GENDER, 
	FIRSTNAME, 
	LASTNAME, 
	MIDDLENAME, 
	MARITALSTATUS, 
	TITLE, 
	REFERENCE, 
	CONTACTNAME, 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID
	FROM EXP_Metadata
),