WITH
SQ_DCLocationStaging AS (
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
		ExtractDate,
		SourceSystemId,
		deleted
	FROM DCLocationStaging
),
EXP_Metadata AS (
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
	ExtractDate,
	SourceSystemId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	deleted,
	-- *INF*: Decode(deleted,'T','1','F','0',NULL)
	Decode(
	    deleted,
	    'T', '1',
	    'F', '0',
	    NULL
	) AS o_deleted
	FROM SQ_DCLocationStaging
),
archDCLocationStaging AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.archDCLocationStaging
	(LocationId, SessionId, LocationXmlId, Description, Address1, Address2, City, County, StateProv, PostalCode, Country, ExtractDate, SourceSystemId, AuditId, deleted)
	SELECT 
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
	COUNTRY, 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	o_AuditId AS AUDITID, 
	o_deleted AS DELETED
	FROM EXP_Metadata
),