WITH
SQ_DC_PartyAssociation AS (
	WITH cte_DCPartyAssociation(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.ObjectId, 
	X.ObjectName, 
	X.PartyId, 
	X.PartyAssociationId, 
	X.SessionId, 
	X.PartyXmlId, 
	X.PartyAssociationType, 
	X.PartyAssociationStatus, 
	X.PartyReference, 
	X.Description, 
	X.EntityType, 
	X.EntityOtherType, 
	X.FederalEmployeeIDNumber, 
	X.CompanyNumber 
	FROM
	DC_PartyAssociation X
	inner join
	cte_DCPartyAssociation Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
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
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_DC_PartyAssociation
),
DCPartyAssociationStaging AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.DCPartyAssociationStaging;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.DCPartyAssociationStaging
	(ObjectId, ObjectName, PartyId, PartyAssociationId, SessionId, PartyXmlId, PartyAssociationType, PartyAssociationStatus, PartyReference, Description, EntityType, EntityOtherType, FederalEmployeeIDNumber, CompanyNumber, ExtractDate, SourceSystemId)
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
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID
	FROM EXP_Metadata
),