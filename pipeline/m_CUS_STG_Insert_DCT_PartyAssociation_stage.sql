WITH
SQ_DC_PartyAssociation AS (
	SELECT DC_PartyAssociation.ObjectId, DC_PartyAssociation.ObjectName, DC_PartyAssociation.PartyId, DC_PartyAssociation.PartyAssociationId, DC_PartyAssociation.SessionId, DC_PartyAssociation.PartyXmlId, DC_PartyAssociation.PartyAssociationType, DC_PartyAssociation.PartyAssociationStatus, DC_PartyAssociation.PartyReference, DC_PartyAssociation.Description, DC_PartyAssociation.EntityType, DC_PartyAssociation.EntityOtherType, DC_PartyAssociation.FederalEmployeeIDNumber, DC_PartyAssociation.CompanyNumber, DC_PartyAssociation.LicensePlateNumber 
	FROM
	DC_PartyAssociation
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session on
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_PartyAssociation.SessionId=@{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session.SessionId
	WHERE
	DC_Session.CreateDateTime >=  '@{pipeline().parameters.SELECTION_START_TS}' 
	and  DC_Session.CreateDateTime <  '@{pipeline().parameters.SELECTION_END_TS}' 
	ORDER BY
	DC_PartyAssociation.SessionId
),
Exp_PartyAssociation AS (
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
	LicensePlateNumber,
	Sysdate AS ExtractDate,
	'DCT' AS SourceSystemID
	FROM SQ_DC_PartyAssociation
),
DCPartyAssociationStage AS (
	INSERT INTO Shortcut_to_DCPartyAssociationStage
	(ExtractDate, SourceSystemid, ObjectId, ObjectName, PartyId, PartyAssociationId, SessionId, PartyXmlId, PartyAssociationType, PartyAssociationStatus, PartyReference, Description, EntityType, EntityOtherType, FederalEmployeeIDNumber, CompanyNumber, LicensePlateNumber)
	SELECT 
	EXTRACTDATE, 
	SourceSystemID AS SOURCESYSTEMID, 
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
	LICENSEPLATENUMBER
	FROM Exp_PartyAssociation
),