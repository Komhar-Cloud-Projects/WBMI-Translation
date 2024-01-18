WITH
SQ_DCPartyAssociationStaging AS (
	SELECT
		ObjectId,
		ObjectName,
		PartyId,
		PartyAssociationId,
		SessionId,
		PartyXmlId,
		PartyAssociationType,
		PartyAssociationStatus,
		PartyReference,
		Description,
		EntityType,
		EntityOtherType,
		FederalEmployeeIDNumber,
		CompanyNumber,
		ExtractDate,
		SourceSystemId
	FROM DCPartyAssociationStaging
),
EXP_Metadata AS (
	SELECT
	ObjectId,
	ObjectName,
	PartyId,
	PartyAssociationId,
	SessionId,
	PartyXmlId,
	PartyAssociationType,
	PartyAssociationStatus,
	PartyReference,
	Description,
	EntityType,
	EntityOtherType,
	FederalEmployeeIDNumber,
	CompanyNumber,
	ExtractDate,
	SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM SQ_DCPartyAssociationStaging
),
archDCPartyAssociationStaging AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.archDCPartyAssociationStaging
	(ObjectId, ObjectName, PartyId, PartyAssociationId, SessionId, PartyXmlId, PartyAssociationType, PartyAssociationStatus, PartyReference, Description, EntityType, EntityOtherType, FederalEmployeeIDNumber, CompanyNumber, ExtractDate, SourceSystemId, AuditId)
	SELECT 
	OBJECTID, 
	OBJECTNAME, 
	PARTYID, 
	PARTYASSOCIATIONID, 
	SESSIONID, 
	PARTYXMLID, 
	PARTYASSOCIATIONTYPE, 
	PARTYASSOCIATIONSTATUS, 
	PARTYREFERENCE, 
	DESCRIPTION, 
	ENTITYTYPE, 
	ENTITYOTHERTYPE, 
	FEDERALEMPLOYEEIDNUMBER, 
	COMPANYNUMBER, 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID
	FROM EXP_Metadata
),