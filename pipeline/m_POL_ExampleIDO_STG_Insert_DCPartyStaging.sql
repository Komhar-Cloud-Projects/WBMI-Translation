WITH
SQ_DC_Party AS (
	WITH cte_DCParty(Sessionid) as
	(select sessionid from @{pipeline().parameters.SOURCE_DATABASE_WB}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_EDWIncrementalDataQualitySessions where ModifiedDate between '@{pipeline().parameters.SELECTION_START_TS}' and '@{pipeline().parameters.SELECTION_END_TS}' 
	AND Autoshred<> '1' 
	 UNION 
	 select distinct A.sessionid from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session A Inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction B on A.SessionID=B.SessionID where B.State<> 'committed' and A.CreateDateTime>='@{pipeline().parameters.SELECTION_START_TS}')
	SELECT 
	X.PartyId, 
	X.SessionId, 
	X.PartyXmlId, 
	X.Type, 
	X.OtherType, 
	X.Name, 
	X.DateOfBirth, 
	X.Gender, 
	X.FirstName, 
	X.LastName, 
	X.MiddleName, 
	X.MaritalStatus, 
	X.Title, 
	X.Reference, 
	X.ContactName 
	FROM
	DC_Party X
	inner join
	cte_DCParty Y on X.Sessionid = Y.Sessionid
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Metadata AS (
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
	sysdate AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	-- *INF*: substr(Name,1,255)
	substr(Name, 1, 255) AS o_Name
	FROM SQ_DC_Party
),
DCPartyStaging AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.DCPartyStaging;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.DCPartyStaging
	(PartyId, SessionId, PartyXmlId, Type, OtherType, Name, DateOfBirth, Gender, FirstName, LastName, MiddleName, MaritalStatus, Title, Reference, ContactName, ExtractDate, SourceSystemId)
	SELECT 
	PARTYID, 
	SESSIONID, 
	PARTYXMLID, 
	TYPE, 
	OTHERTYPE, 
	o_Name AS NAME, 
	DATEOFBIRTH, 
	GENDER, 
	FIRSTNAME, 
	LASTNAME, 
	MIDDLENAME, 
	MARITALSTATUS, 
	TITLE, 
	REFERENCE, 
	CONTACTNAME, 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID
	FROM EXP_Metadata
),