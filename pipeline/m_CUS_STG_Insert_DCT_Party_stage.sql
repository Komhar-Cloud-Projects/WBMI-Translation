WITH
SQ_DC_Party AS (
	SELECT DC_Party.PartyId, DC_Party.SessionId, DC_Party.PartyXmlId, DC_Party.Type, DC_Party.OtherType, DC_Party.Name, DC_Party.DateOfBirth, DC_Party.Gender, DC_Party.FirstName, DC_Party.LastName, DC_Party.MiddleName, DC_Party.MaritalStatus, DC_Party.Title, DC_Party.Reference, DC_Party.ContactName 
	FROM
	 DC_Party
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session on
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Party.SessionId=@{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session.SessionId
	WHERE
	DC_Session.CreateDateTime >=  '@{pipeline().parameters.SELECTION_START_TS}' 
	and
	DC_Session.CreateDateTime <  '@{pipeline().parameters.SELECTION_END_TS}' 
	ORDER BY
	DC_Party.SessionId
),
Exp_Party AS (
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
	Sysdate AS ExtractDate,
	'DCT' AS SourceSystemID
	FROM SQ_DC_Party
),
DCPartyStage AS (
	INSERT INTO Shortcut_to_DCPartyStage
	(ExtractDate, SourceSystemid, PartyId, SessionId, PartyXmlId, Type, OtherType, Name, DateOfBirth, Gender, FirstName, LastName, MiddleName, MaritalStatus, Title, Reference, ContactName)
	SELECT 
	EXTRACTDATE, 
	SourceSystemID AS SOURCESYSTEMID, 
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
	CONTACTNAME
	FROM Exp_Party
),