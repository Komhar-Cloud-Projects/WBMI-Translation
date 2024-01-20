WITH
SQ_WorkDCTLocation AS (
	SELECT
		WorkDCTLocationId,
		ExtractDate,
		SourceSystemId,
		LocationAssociationId,
		LocationAssociationObjectId,
		LocationAssociationObjectName,
		LocationId,
		SessionId,
		Territory,
		LocationNumber,
		LocationXmlId,
		StateProvince,
		PostalCode,
		City,
		County,
		Address1,
		LocationDescription,
		PrimaryStateLocationIndicator,
		PrimaryRatingLocationIndicator,
		Address2,
		Country,
		LocationAssociationType
	FROM WorkDCTLocation
),
EXp_Default AS (
	SELECT
	ExtractDate,
	SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	WorkDCTLocationId,
	LocationAssociationId,
	LocationAssociationObjectId,
	LocationAssociationObjectName,
	LocationId,
	SessionId,
	Territory,
	LocationNumber,
	LocationXmlId,
	StateProvince,
	PostalCode,
	City,
	County,
	Address1,
	LocationDescription,
	PrimaryStateLocationIndicator,
	-- *INF*: DECODE(PrimaryStateLocationIndicator, 'T', 1, 'F', 0, NULL)
	DECODE(
	    PrimaryStateLocationIndicator,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_PrimaryStateLocationIndicator,
	PrimaryRatingLocationIndicator,
	-- *INF*: DECODE(PrimaryRatingLocationIndicator, 'T', 1, 'F', 0, NULL)
	DECODE(
	    PrimaryRatingLocationIndicator,
	    'T', 1,
	    'F', 0,
	    NULL
	) AS o_PrimaryRatingLocationIndicator,
	Address2,
	Country,
	LocationAssociationType
	FROM SQ_WorkDCTLocation
),
ArchWorkDCTLocation AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ArchWorkDCTLocation
	(ExtractDate, SourceSystemId, AuditId, WorkDCTLocationId, LocationAssociationId, LocationAssociationObjectId, LocationAssociationObjectName, LocationId, SessionId, Territory, LocationNumber, LocationXmlId, StateProvince, PostalCode, City, County, Address1, LocationDescription, PrimaryStateLocationIndicator, PrimaryRatingLocationIndicator, Address2, Country, LocationAssociationType)
	SELECT 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	audit_id AS AUDITID, 
	WORKDCTLOCATIONID, 
	LOCATIONASSOCIATIONID, 
	LOCATIONASSOCIATIONOBJECTID, 
	LOCATIONASSOCIATIONOBJECTNAME, 
	LOCATIONID, 
	SESSIONID, 
	TERRITORY, 
	LOCATIONNUMBER, 
	LOCATIONXMLID, 
	STATEPROVINCE, 
	POSTALCODE, 
	CITY, 
	COUNTY, 
	ADDRESS1, 
	LOCATIONDESCRIPTION, 
	o_PrimaryStateLocationIndicator AS PRIMARYSTATELOCATIONINDICATOR, 
	o_PrimaryRatingLocationIndicator AS PRIMARYRATINGLOCATIONINDICATOR, 
	ADDRESS2, 
	COUNTRY, 
	LOCATIONASSOCIATIONTYPE
	FROM EXp_Default
),