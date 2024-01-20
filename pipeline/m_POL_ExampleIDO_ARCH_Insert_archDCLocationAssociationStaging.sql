WITH
SQ_DCLocationAssociationStaging AS (
	SELECT
		ObjectId,
		ObjectName,
		LocationId,
		LocationAssociationId,
		SessionId,
		LocationXmlId,
		LocationAssociationType,
		ExtractDate,
		SourceSystemId
	FROM DCLocationAssociationStaging
),
EXP_Metadata AS (
	SELECT
	ObjectId,
	ObjectName,
	LocationId,
	LocationAssociationId,
	SessionId,
	LocationXmlId,
	LocationAssociationType,
	ExtractDate,
	SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId
	FROM SQ_DCLocationAssociationStaging
),
archDCLocationAssociationStaging AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.archDCLocationAssociationStaging
	(ObjectId, ObjectName, LocationId, LocationAssociationId, SessionId, LocationXmlId, LocationAssociationType, ExtractDate, SourceSystemId, AuditId)
	SELECT 
	OBJECTID, 
	OBJECTNAME, 
	LOCATIONID, 
	LOCATIONASSOCIATIONID, 
	SESSIONID, 
	LOCATIONXMLID, 
	LOCATIONASSOCIATIONTYPE, 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID
	FROM EXP_Metadata
),