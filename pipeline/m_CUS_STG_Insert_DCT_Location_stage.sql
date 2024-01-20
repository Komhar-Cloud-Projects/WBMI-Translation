WITH
SQ_DC_Location AS (
	SELECT DC_Location.LocationId, DC_Location.SessionId, DC_Location.LocationXmlId, DC_Location.Description, DC_Location.Address1, DC_Location.Address2, DC_Location.City, DC_Location.County, DC_Location.StateProv, DC_Location.PostalCode, DC_Location.Country 
	FROM
	DC_Location
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session on
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Location.SessionId=@{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session.SessionId
	WHERE
	DC_Session.CreateDateTime >=  '@{pipeline().parameters.SELECTION_START_TS}' 
	and
	DC_Session.CreateDateTime <'@{pipeline().parameters.SELECTION_END_TS}'
	ORDER BY
	DC_Location.SessionId
),
Exp_Location AS (
	SELECT
	LocationId,
	SessionId,
	LocationXmlId,
	Description,
	Address1,
	Address2,
	City,
	County,
	StateProv,
	PostalCode,
	Country,
	Sysdate AS ExtractDate,
	'DCT' AS SourceSystemID
	FROM SQ_DC_Location
),
DCLocationStage AS (
	INSERT INTO Shortcut_to_DCLocationStage
	(ExtractDate, SourceSystemid, LocationId, SessionId, LocationXmlId, Description, Address1, Address2, City, County, StateProv, PostalCode, Country)
	SELECT 
	EXTRACTDATE, 
	SourceSystemID AS SOURCESYSTEMID, 
	LOCATIONID, 
	SESSIONID, 
	LOCATIONXMLID, 
	DESCRIPTION, 
	ADDRESS1, 
	ADDRESS2, 
	CITY, 
	COUNTY, 
	STATEPROV, 
	POSTALCODE, 
	COUNTRY
	FROM Exp_Location
),