WITH
SQ_WorkDCTLocation AS (
	SELECT b.LocationAssociationId,
		b.ObjectId,
		b.ObjectName,
		c.LocationId,
		b.SessionId,
		CASE b.ObjectName
			WHEN 'DC_GL_Location'
				THEN (
						SELECT cast(Territory AS VARCHAR(128))
						FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCGLLocationStaging gl
						WHERE gl.SessionId = b.sessionid
							AND gl.GL_LocationId = b.objectid
						)
			WHEN 'DC_CA_Location'
				THEN (
						SELECT cast(Territory AS VARCHAR(128))
						FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCCALocationStaging cal
						WHERE cal.SessionId = b.sessionid
							AND cal.CA_LocationId = b.objectid
						)
			WHEN 'DC_BP_Location'
				THEN (
						SELECT cast(Territory AS VARCHAR(128))
						FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCBPLocationStage bpl
						WHERE bpl.SessionId = b.sessionid
							AND bpl.BPLocationId = b.objectid
						)
			ELSE 'N/A'
			END AS Territory,
		d.LocationNumber,
		c.LocationXmlId,
		c.StateProv StateProvince,
		c.PostalCode,
		c.City,
		c.County,
		c.Address1,
		c.Description LocationDescription,
		'1' As PrimaryStateLocationIndicator,
		'1' AS PrimaryRatingLocationIndicator,
		c.Address2,
		c.Country,
		LocationAssociationType
	FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCLocationAssociationStaging b
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DCLocationStaging c
		ON b.LocationId = c.LocationId
			AND b.SessionId = c.SessionId
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBLocationStaging d
		ON c.LocationId = d.LocationId
			AND c.SessionId = d.SessionId
	ORDER BY LocationAssociationId
),
EXP_Default AS (
	SELECT
	SYSDATE AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
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
	FROM SQ_WorkDCTLocation
),
WorkDCTLocation AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.WorkDCTLocation;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.WorkDCTLocation
	(ExtractDate, SourceSystemId, LocationAssociationId, LocationAssociationObjectId, LocationAssociationObjectName, LocationId, SessionId, Territory, LocationNumber, LocationXmlId, StateProvince, PostalCode, City, County, Address1, LocationDescription, PrimaryStateLocationIndicator, PrimaryRatingLocationIndicator, Address2, Country, LocationAssociationType)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
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
	PRIMARYSTATELOCATIONINDICATOR, 
	PRIMARYRATINGLOCATIONINDICATOR, 
	ADDRESS2, 
	COUNTRY, 
	LOCATIONASSOCIATIONTYPE
	FROM EXP_Default
),