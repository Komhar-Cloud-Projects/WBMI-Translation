WITH
SQ_DC_LocationAssociation AS (
	WITH cte_DCLocationAssociation(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.ObjectId, 
	X.ObjectName, 
	X.LocationId, 
	X.LocationAssociationId, 
	X.SessionId, 
	X.LocationXmlId, 
	X.LocationAssociationType 
	FROM
	DC_LocationAssociation X
	inner join
	cte_DCLocationAssociation Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
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
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId
	FROM SQ_DC_LocationAssociation
),
DCLocationAssociationStaging AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.DCLocationAssociationStaging;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.DCLocationAssociationStaging
	(ObjectId, ObjectName, LocationId, LocationAssociationId, SessionId, LocationXmlId, LocationAssociationType, ExtractDate, SourceSystemId)
	SELECT 
	OBJECTID, 
	OBJECTNAME, 
	LOCATIONID, 
	LOCATIONASSOCIATIONID, 
	SESSIONID, 
	LOCATIONXMLID, 
	LOCATIONASSOCIATIONTYPE, 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID
	FROM EXP_Metadata
),