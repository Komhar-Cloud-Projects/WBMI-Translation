WITH
SQ_DC_LocationAssociation AS (
	SELECT DC_LocationAssociation.ObjectId, DC_LocationAssociation.ObjectName, DC_LocationAssociation.LocationId, DC_LocationAssociation.LocationAssociationId, DC_LocationAssociation.SessionId, DC_LocationAssociation.LocationXmlId, DC_LocationAssociation.LocationAssociationType 
	FROM
	DC_LocationAssociation
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session on
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_LocationAssociation.SessionId=@{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session.SessionId
	WHERE
	DC_Session.CreateDateTime >=  '@{pipeline().parameters.SELECTION_START_TS}' 
	and 
	DC_Session.CreateDateTime <  '@{pipeline().parameters.SELECTION_END_TS}'
	ORDER BY
	DC_LocationAssociation.SessionId
),
Exp_LocationAssociation AS (
	SELECT
	ObjectId,
	ObjectName,
	LocationId,
	LocationAssociationId,
	SessionId,
	LocationXmlId,
	LocationAssociationType,
	Sysdate AS ExtractDate,
	'DCT' AS SourceSystemID
	FROM SQ_DC_LocationAssociation
),
DCLocationAssociationStage AS (
	INSERT INTO Shortcut_to_DCLocationAssociationStage
	(ExtractDate, SourceSystemid, ObjectId, ObjectName, LocationId, LocationAssociationId, SessionId, LocationXmlId, LocationAssociationType)
	SELECT 
	EXTRACTDATE, 
	SourceSystemID AS SOURCESYSTEMID, 
	OBJECTID, 
	OBJECTNAME, 
	LOCATIONID, 
	LOCATIONASSOCIATIONID, 
	SESSIONID, 
	LOCATIONXMLID, 
	LOCATIONASSOCIATIONTYPE
	FROM Exp_LocationAssociation
),